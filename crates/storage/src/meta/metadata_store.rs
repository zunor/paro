use parking_lot::RwLock;
use paro_common::error::{self as paro_error, Result};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Batch operation for metadata updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataOp {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

/// Persistent metadata storage abstraction.
pub trait MetadataStore: Send + Sync {
    fn put(&self, key: &str, value: &[u8]) -> Result<()>;
    fn durable_put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.put(key, value)
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn write_batch(&self, ops: &[MetadataOp]) -> Result<()>;
    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
    fn exists(&self, key: &str) -> Result<bool>;
}

#[derive(Debug)]
struct StagedPut {
    tmp_path: PathBuf,
    final_path: PathBuf,
}

/// MetadataStore implementation backed by files under a root directory.
///
/// Put/write_batch follow a temp-file + rename flow for atomic replacement.
#[derive(Debug)]
pub struct FileMetadataStore {
    root_dir: PathBuf,
    op_lock: RwLock<()>,
    temp_counter: AtomicU64,
}

#[cfg(any(test, debug_assertions))]
use std::sync::{LazyLock, Mutex};
#[cfg(any(test, debug_assertions))]
static FAIL_METADATA_RENAME: LazyLock<Mutex<Vec<MetadataFailpoint>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));
#[cfg(any(test, debug_assertions))]
static FAIL_METADATA_PARENT_SYNC: LazyLock<Mutex<Vec<MetadataFailpoint>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

#[cfg(any(test, debug_assertions))]
#[derive(Debug, Clone)]
struct MetadataFailpoint {
    remaining_calls: usize,
    target_path: Option<PathBuf>,
}

impl FileMetadataStore {
    pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let root_dir = root_dir.into();
        fs::create_dir_all(&root_dir).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create metadata root directory {:?}: {}",
                root_dir, e
            ))
        })?;

        let root_dir = root_dir.canonicalize().unwrap_or(root_dir);
        Ok(Self {
            root_dir,
            op_lock: RwLock::new(()),
            temp_counter: AtomicU64::new(0),
        })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    fn validate_key(key: &str) -> Result<Vec<&str>> {
        if key.is_empty() {
            return Err(paro_error::invalid_input("metadata key cannot be empty"));
        }
        if key.starts_with('/') {
            return Err(paro_error::invalid_input(format!(
                "metadata key cannot be absolute: {}",
                key
            )));
        }

        let segments: Vec<&str> = key.split('/').collect();
        for segment in &segments {
            if segment.is_empty() {
                return Err(paro_error::invalid_input(format!(
                    "metadata key contains empty path segment: {}",
                    key
                )));
            }
            if *segment == "." || *segment == ".." {
                return Err(paro_error::invalid_input(format!(
                    "metadata key contains invalid path segment '{}': {}",
                    segment, key
                )));
            }
            if segment.contains('\\') {
                return Err(paro_error::invalid_input(format!(
                    "metadata key contains invalid separator '\\': {}",
                    key
                )));
            }
        }

        Ok(segments)
    }

    fn validate_prefix(prefix: &str) -> Result<()> {
        if prefix.is_empty() {
            return Ok(());
        }
        if prefix.starts_with('/') {
            return Err(paro_error::invalid_input(format!(
                "metadata prefix cannot be absolute: {}",
                prefix
            )));
        }
        if prefix.contains('\\') {
            return Err(paro_error::invalid_input(format!(
                "metadata prefix contains invalid separator '\\': {}",
                prefix
            )));
        }

        for segment in prefix.split('/') {
            if segment == "." || segment == ".." {
                return Err(paro_error::invalid_input(format!(
                    "metadata prefix contains invalid segment '{}': {}",
                    segment, prefix
                )));
            }
        }
        Ok(())
    }

    fn extension_for_key(key: &str) -> &'static str {
        if key == "catalog"
            || key == "run_state"
            || key.starts_with("config/")
            || key.starts_with("manifest/")
        {
            "json"
        } else {
            "bin"
        }
    }

    fn key_to_relative_path(key: &str) -> Result<PathBuf> {
        let segments = Self::validate_key(key)?;
        let ext = Self::extension_for_key(key);
        let mut relative = PathBuf::new();

        for segment in &segments[..segments.len() - 1] {
            relative.push(segment);
        }
        relative.push(format!("{}.{}", segments[segments.len() - 1], ext));
        Ok(relative)
    }

    fn relative_path_to_key(path: &Path) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        for component in path.components() {
            match component {
                Component::Normal(name) => parts.push(name.to_string_lossy().into_owned()),
                _ => return None,
            }
        }

        if parts.is_empty() {
            return None;
        }

        let file_name = parts.pop()?;
        if file_name.contains(".tmp-") {
            return None;
        }

        let stem = if let Some(stem) = file_name.strip_suffix(".bin") {
            stem
        } else if let Some(stem) = file_name.strip_suffix(".json") {
            stem
        } else {
            return None;
        };

        if stem.is_empty() {
            return None;
        }

        parts.push(stem.to_string());
        Some(parts.join("/"))
    }

    fn data_path_for_key(&self, key: &str) -> Result<PathBuf> {
        let relative = Self::key_to_relative_path(key)?;
        Ok(self.root_dir.join(relative))
    }

    fn ensure_parent_dir(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create metadata parent directory {:?}: {}",
                    parent, e
                ))
            })?;
        }
        Ok(())
    }

    fn next_tmp_path(&self, target: &Path) -> PathBuf {
        let sequence = self.temp_counter.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        let base_name = target
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_else(|| "metadata".to_string());
        target.with_file_name(format!("{}.tmp-{}-{}-{}", base_name, pid, nanos, sequence))
    }

    fn write_tmp_file(path: &Path, value: &[u8]) -> Result<()> {
        let mut file = File::create(path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create temp metadata file {:?}: {}",
                path, e
            ))
        })?;
        file.write_all(value).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to write temp metadata file {:?}: {}",
                path, e
            ))
        })?;
        file.sync_all().map_err(|e| {
            paro_error::io_error(format!(
                "Failed to fsync temp metadata file {:?}: {}",
                path, e
            ))
        })?;
        Ok(())
    }

    fn remove_file_if_exists(path: &Path) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(paro_error::io_error(format!(
                "Failed to remove metadata file {:?}: {}",
                path, err
            ))),
        }
    }

    fn cleanup_tmp_files(paths: &[PathBuf]) {
        for path in paths {
            let _ = fs::remove_file(path);
        }
    }

    fn cleanup_remaining_tmp_files(staged_puts: &[StagedPut], from_put_index: usize) {
        let pending: Vec<PathBuf> = staged_puts[from_put_index..]
            .iter()
            .map(|put| put.tmp_path.clone())
            .collect();
        Self::cleanup_tmp_files(&pending);
    }

    fn cleanup_empty_parent_dirs(&self, start: &Path) -> Result<()> {
        let mut current = start.to_path_buf();
        while current.starts_with(&self.root_dir) && current != self.root_dir {
            match fs::remove_dir(&current) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) if err.kind() == io::ErrorKind::DirectoryNotEmpty => break,
                Err(err) => {
                    return Err(paro_error::io_error(format!(
                        "Failed to cleanup metadata directory {:?}: {}",
                        current, err
                    )))
                }
            }

            let Some(parent) = current.parent() else {
                break;
            };
            current = parent.to_path_buf();
        }
        Ok(())
    }

    fn stage_put(&self, key: &str, value: &[u8]) -> Result<StagedPut> {
        let final_path = self.data_path_for_key(key)?;
        Self::ensure_parent_dir(&final_path)?;
        let tmp_path = self.next_tmp_path(&final_path);
        Self::write_tmp_file(&tmp_path, value)?;
        Ok(StagedPut {
            tmp_path,
            final_path,
        })
    }

    fn rename_staged_put(staged_put: &StagedPut) -> Result<()> {
        #[cfg(any(test, debug_assertions))]
        if should_fail_for_path(&FAIL_METADATA_RENAME, &staged_put.final_path) {
            return Err(paro_error::io_error(format!(
                "Simulated metadata rename failure {:?} -> {:?}",
                staged_put.tmp_path, staged_put.final_path
            )));
        }
        fs::rename(&staged_put.tmp_path, &staged_put.final_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to commit metadata temp file {:?} -> {:?}: {}",
                staged_put.tmp_path, staged_put.final_path, e
            ))
        })
    }

    fn sync_parent_dir(path: &Path) -> Result<()> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        #[cfg(any(test, debug_assertions))]
        if should_fail_for_path(&FAIL_METADATA_PARENT_SYNC, path) {
            return Err(paro_error::io_error(format!(
                "Simulated metadata parent directory fsync failure {:?}",
                parent
            )));
        }
        let dir = File::open(parent).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to open metadata parent directory {:?} for fsync: {}",
                parent, e
            ))
        })?;
        dir.sync_all().map_err(|e| {
            paro_error::io_error(format!(
                "Failed to fsync metadata parent directory {:?}: {}",
                parent, e
            ))
        })
    }

    fn collect_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(dir).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to read metadata directory {:?}: {}",
                dir, e
            ))
        })? {
            let entry = entry.map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to iterate metadata directory {:?}: {}",
                    dir, e
                ))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| {
                paro_error::io_error(format!("Failed to inspect metadata path {:?}: {}", path, e))
            })?;
            if file_type.is_dir() {
                Self::collect_files_recursive(&path, out)?;
            } else if file_type.is_file() {
                out.push(path);
            }
        }
        Ok(())
    }
}

impl MetadataStore for FileMetadataStore {
    fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let _guard = self.op_lock.write();
        let staged_put = self.stage_put(key, value)?;
        if let Err(err) = Self::rename_staged_put(&staged_put) {
            Self::cleanup_tmp_files(&[staged_put.tmp_path]);
            return Err(err);
        }
        Ok(())
    }

    fn durable_put(&self, key: &str, value: &[u8]) -> Result<()> {
        let _guard = self.op_lock.write();
        let staged_put = self.stage_put(key, value)?;
        if let Err(err) = Self::rename_staged_put(&staged_put) {
            Self::cleanup_tmp_files(&[staged_put.tmp_path]);
            return Err(err);
        }
        Self::sync_parent_dir(&staged_put.final_path)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let _guard = self.op_lock.read();
        let path = self.data_path_for_key(key)?;
        match fs::read(&path) {
            Ok(value) => Ok(Some(value)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(paro_error::io_error(format!(
                "Failed to read metadata key '{}' from {:?}: {}",
                key, path, err
            ))),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let _guard = self.op_lock.write();
        let path = self.data_path_for_key(key)?;
        Self::remove_file_if_exists(&path)?;
        if let Some(parent) = path.parent() {
            self.cleanup_empty_parent_dirs(parent)?;
        }
        Ok(())
    }

    fn write_batch(&self, ops: &[MetadataOp]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let _guard = self.op_lock.write();

        let mut staged_puts: Vec<StagedPut> = Vec::new();
        for op in ops {
            match op {
                MetadataOp::Put { key, value } => match self.stage_put(key, value) {
                    Ok(staged_put) => staged_puts.push(staged_put),
                    Err(err) => {
                        let cleanup: Vec<PathBuf> = staged_puts
                            .iter()
                            .map(|staged_put| staged_put.tmp_path.clone())
                            .collect();
                        Self::cleanup_tmp_files(&cleanup);
                        return Err(err);
                    }
                },
                MetadataOp::Delete { key } => {
                    if let Err(err) = self.data_path_for_key(key) {
                        let cleanup: Vec<PathBuf> = staged_puts
                            .iter()
                            .map(|staged_put| staged_put.tmp_path.clone())
                            .collect();
                        Self::cleanup_tmp_files(&cleanup);
                        return Err(err);
                    }
                }
            }
        }

        let mut put_index = 0;
        for op in ops {
            match op {
                MetadataOp::Put { .. } => {
                    if let Err(err) = Self::rename_staged_put(&staged_puts[put_index]) {
                        Self::cleanup_remaining_tmp_files(&staged_puts, put_index);
                        return Err(err);
                    }
                    put_index += 1;
                }
                MetadataOp::Delete { key } => {
                    let path = self.data_path_for_key(key)?;
                    if let Err(err) = Self::remove_file_if_exists(&path) {
                        Self::cleanup_remaining_tmp_files(&staged_puts, put_index);
                        return Err(err);
                    }
                    if let Some(parent) = path.parent() {
                        if let Err(err) = self.cleanup_empty_parent_dirs(parent) {
                            Self::cleanup_remaining_tmp_files(&staged_puts, put_index);
                            return Err(err);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        Self::validate_prefix(prefix)?;

        let _guard = self.op_lock.read();
        if !self.root_dir.exists() {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();
        Self::collect_files_recursive(&self.root_dir, &mut files)?;

        let mut result: Vec<(String, Vec<u8>)> = Vec::new();
        for path in files {
            let relative = path.strip_prefix(&self.root_dir).map_err(|e| {
                paro_error::internal(format!(
                    "Failed to compute relative path for metadata file {:?}: {}",
                    path, e
                ))
            })?;

            let Some(key) = Self::relative_path_to_key(relative) else {
                continue;
            };
            if !key.starts_with(prefix) {
                continue;
            }

            let value = fs::read(&path).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to read metadata file {:?} during scan: {}",
                    path, e
                ))
            })?;
            result.push((key, value));
        }

        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let _guard = self.op_lock.read();
        let path = self.data_path_for_key(key)?;
        Ok(path.is_file())
    }
}

#[cfg(any(test, debug_assertions))]
fn arm_failpoint(
    slot: &LazyLock<Mutex<Vec<MetadataFailpoint>>>,
    target_path: Option<PathBuf>,
    nth_call: usize,
) {
    assert!(nth_call > 0, "metadata failpoint call index must be > 0");
    let mut failpoints = slot.lock().unwrap();
    failpoints.push(MetadataFailpoint {
        remaining_calls: nth_call,
        target_path: target_path.map(|path| normalize_failpoint_path(&path)),
    });
}

#[cfg(any(test, debug_assertions))]
fn should_fail_for_path(slot: &LazyLock<Mutex<Vec<MetadataFailpoint>>>, path: &Path) -> bool {
    let normalized_path = normalize_failpoint_path(path);
    let mut failpoints = slot.lock().unwrap();
    if failpoints.is_empty() {
        return false;
    }

    for matcher in [Some(normalized_path.as_path()), None] {
        let Some(index) =
            failpoints
                .iter()
                .position(|spec| match (matcher, spec.target_path.as_deref()) {
                    (Some(target), Some(spec_target)) => spec_target == target,
                    (None, None) => true,
                    _ => false,
                })
        else {
            continue;
        };

        let spec = &mut failpoints[index];
        spec.remaining_calls = spec.remaining_calls.saturating_sub(1);
        if spec.remaining_calls == 0 {
            failpoints.remove(index);
            return true;
        }
        return false;
    }

    false
}

#[cfg(any(test, debug_assertions))]
fn normalize_failpoint_path(path: &Path) -> PathBuf {
    if path.exists() {
        return path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    }

    let mut missing_suffix = Vec::new();
    let mut current = path;
    while !current.exists() {
        let Some(name) = current.file_name() else {
            return path.to_path_buf();
        };
        missing_suffix.push(name.to_os_string());
        let Some(parent) = current.parent() else {
            return path.to_path_buf();
        };
        current = parent;
    }

    let mut normalized = current
        .canonicalize()
        .unwrap_or_else(|_| current.to_path_buf());
    for segment in missing_suffix.iter().rev() {
        normalized.push(segment);
    }
    normalized
}

#[cfg(any(test, debug_assertions))]
#[doc(hidden)]
pub mod testing {
    use super::{arm_failpoint, FAIL_METADATA_PARENT_SYNC, FAIL_METADATA_RENAME};
    use std::path::{Path, PathBuf};

    pub fn arm_next_metadata_rename_failure() {
        arm_metadata_rename_failure_on_nth_call(1);
    }

    pub fn arm_metadata_rename_failure_on_nth_call(n: usize) {
        arm_failpoint(&FAIL_METADATA_RENAME, None, n);
    }

    pub fn arm_metadata_rename_failure_for_path_on_nth_call(path: impl AsRef<Path>, n: usize) {
        arm_failpoint(&FAIL_METADATA_RENAME, Some(PathBuf::from(path.as_ref())), n);
    }

    pub fn arm_next_metadata_parent_sync_failure() {
        arm_metadata_parent_sync_failure_on_nth_call(1);
    }

    pub fn arm_metadata_parent_sync_failure_on_nth_call(n: usize) {
        arm_failpoint(&FAIL_METADATA_PARENT_SYNC, None, n);
    }

    pub fn arm_metadata_parent_sync_failure_for_path_on_nth_call(path: impl AsRef<Path>, n: usize) {
        arm_failpoint(
            &FAIL_METADATA_PARENT_SYNC,
            Some(PathBuf::from(path.as_ref())),
            n,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    fn arm_metadata_rename_failure_for_key(store: &FileMetadataStore, key: &str) {
        testing::arm_metadata_rename_failure_for_path_on_nth_call(
            store.data_path_for_key(key).unwrap(),
            1,
        );
    }

    fn arm_metadata_parent_sync_failure_for_key(store: &FileMetadataStore, key: &str) {
        testing::arm_metadata_parent_sync_failure_for_path_on_nth_call(
            store.data_path_for_key(key).unwrap(),
            1,
        );
    }

    fn collect_tmp_files(dir: &Path, tmp_files: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_tmp_files(&path, tmp_files);
                continue;
            }
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if file_name.contains(".tmp-") {
                    tmp_files.push(path);
                }
            }
        }
    }

    #[test]
    fn metadata_store_crud_test() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        store.put("tablet/2/meta", b"value-2").unwrap();
        store.put("tablet/1/meta", b"value-1").unwrap();
        store
            .put("config/storage", br#"{"page_size":262144}"#)
            .unwrap();

        assert!(temp_dir.path().join("tablet/1/meta.bin").exists());
        assert!(temp_dir.path().join("tablet/2/meta.bin").exists());
        assert!(temp_dir.path().join("config/storage.json").exists());

        assert_eq!(
            store.get("tablet/1/meta").unwrap(),
            Some(b"value-1".to_vec())
        );
        assert!(store.exists("tablet/2/meta").unwrap());

        let scan = store.scan_prefix("tablet/").unwrap();
        let keys: Vec<String> = scan.into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, vec!["tablet/1/meta", "tablet/2/meta"]);

        store.delete("tablet/1/meta").unwrap();
        assert!(!store.exists("tablet/1/meta").unwrap());
        assert_eq!(store.get("tablet/1/meta").unwrap(), None);
    }

    #[test]
    fn durable_put_uses_catalog_json_without_leaking_tmp_files() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        store
            .durable_put("catalog", br#"{"format_version":1}"#)
            .unwrap();

        assert!(temp_dir.path().join("catalog.json").exists());
        assert_eq!(
            store.get("catalog").unwrap(),
            Some(br#"{"format_version":1}"#.to_vec())
        );

        let mut tmp_files = Vec::new();
        collect_tmp_files(temp_dir.path(), &mut tmp_files);
        assert!(
            tmp_files.is_empty(),
            "durable_put should not leave tmp files behind"
        );
    }

    #[test]
    fn durable_put_keeps_previous_catalog_when_rename_fails_before_commit() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        store
            .durable_put("catalog", br#"{"version":"old"}"#)
            .expect("persist initial catalog");

        arm_metadata_rename_failure_for_key(&store, "catalog");
        let err = store
            .durable_put("catalog", br#"{"version":"new"}"#)
            .expect_err("rename failpoint should make durable_put fail");
        assert!(
            err.to_string()
                .contains("Simulated metadata rename failure"),
            "rename failure should propagate to the caller"
        );
        assert_eq!(
            store.get("catalog").unwrap(),
            Some(br#"{"version":"old"}"#.to_vec()),
            "catalog should still expose the previous committed blob when rename never committed"
        );

        let mut tmp_files = Vec::new();
        collect_tmp_files(temp_dir.path(), &mut tmp_files);
        assert!(
            tmp_files.is_empty(),
            "failed rename should still clean up temporary files"
        );
    }

    #[test]
    fn durable_put_reports_parent_sync_failure_after_rename() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        store
            .durable_put("catalog", br#"{"version":"old"}"#)
            .expect("persist initial catalog");

        arm_metadata_parent_sync_failure_for_key(&store, "catalog");
        let err = store
            .durable_put("catalog", br#"{"version":"new"}"#)
            .expect_err("parent sync failpoint should make durable_put fail");
        assert!(
            err.to_string()
                .contains("Simulated metadata parent directory fsync failure"),
            "durable_put must surface missing parent-dir fsync as a failure"
        );
        assert_eq!(
            store.get("catalog").unwrap(),
            Some(br#"{"version":"new"}"#.to_vec()),
            "rename may already have replaced the blob, but the caller must still see a failure"
        );

        let mut tmp_files = Vec::new();
        collect_tmp_files(temp_dir.path(), &mut tmp_files);
        assert!(
            tmp_files.is_empty(),
            "parent sync failure should not leak temp files"
        );
    }

    #[test]
    fn metadata_store_batch_atomicity_test() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        let ops = vec![
            MetadataOp::Put {
                key: "tablet/8/rowset/1".to_string(),
                value: b"rs-1".to_vec(),
            },
            MetadataOp::Put {
                key: "tablet/8/rowset/2".to_string(),
                value: b"rs-2".to_vec(),
            },
        ];
        store.write_batch(&ops).unwrap();

        assert_eq!(
            store.get("tablet/8/rowset/1").unwrap(),
            Some(b"rs-1".to_vec())
        );
        assert_eq!(
            store.get("tablet/8/rowset/2").unwrap(),
            Some(b"rs-2".to_vec())
        );

        let update_ops = vec![
            MetadataOp::Delete {
                key: "tablet/8/rowset/1".to_string(),
            },
            MetadataOp::Put {
                key: "tablet/8/rowset/3".to_string(),
                value: b"rs-3".to_vec(),
            },
        ];
        store.write_batch(&update_ops).unwrap();

        assert_eq!(store.get("tablet/8/rowset/1").unwrap(), None);
        assert_eq!(
            store.get("tablet/8/rowset/3").unwrap(),
            Some(b"rs-3".to_vec())
        );
    }

    #[test]
    fn metadata_store_concurrent_access_test() {
        let temp_dir = tempdir().unwrap();
        let store = Arc::new(FileMetadataStore::new(temp_dir.path()).unwrap());

        let mut handles = Vec::new();
        for worker in 0..4 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("tablet/{}/rowset/{}", worker, i);
                    let value = format!("{}:{}", worker, i).into_bytes();
                    store.put(&key, &value).unwrap();
                    assert_eq!(store.get(&key).unwrap(), Some(value));
                }
            }));
        }

        {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let scan = store.scan_prefix("tablet/").unwrap();
                    let mut prev: Option<String> = None;
                    for (key, _) in scan {
                        if let Some(prev_key) = prev.as_ref() {
                            assert!(prev_key <= &key);
                        }
                        prev = Some(key);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.scan_prefix("tablet/").unwrap().len(), 400);
    }

    #[test]
    fn metadata_store_exception_recovery_test() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        fs::write(temp_dir.path().join("conflict"), b"not-a-directory").unwrap();

        let ops = vec![
            MetadataOp::Put {
                key: "tablet/9/meta".to_string(),
                value: b"meta".to_vec(),
            },
            MetadataOp::Put {
                key: "conflict/child".to_string(),
                value: b"bad".to_vec(),
            },
        ];

        let err = store.write_batch(&ops);
        assert!(err.is_err());

        assert_eq!(store.get("tablet/9/meta").unwrap(), None);
        assert!(!store.exists("tablet/9/meta").unwrap());

        let mut tmp_files = Vec::new();
        collect_tmp_files(temp_dir.path(), &mut tmp_files);
        assert!(
            tmp_files.is_empty(),
            "temporary files were not cleaned up: {:?}",
            tmp_files
        );

        store.put("tablet/9/meta", b"meta").unwrap();
        assert_eq!(store.get("tablet/9/meta").unwrap(), Some(b"meta".to_vec()));
    }

    #[test]
    fn metadata_store_batch_invalid_key_cleanup_test() {
        let temp_dir = tempdir().unwrap();
        let store = FileMetadataStore::new(temp_dir.path()).unwrap();

        let ops = vec![
            MetadataOp::Put {
                key: "tablet/10/meta".to_string(),
                value: b"meta".to_vec(),
            },
            MetadataOp::Delete {
                key: "../invalid".to_string(),
            },
        ];

        let err = store.write_batch(&ops);
        assert!(err.is_err());
        assert_eq!(store.get("tablet/10/meta").unwrap(), None);

        let mut tmp_files = Vec::new();
        collect_tmp_files(temp_dir.path(), &mut tmp_files);
        assert!(tmp_files.is_empty());
    }
}
