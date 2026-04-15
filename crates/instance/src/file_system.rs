//! File system abstraction for database operations.
//!
//! Supports both local disk access and in-memory test doubles.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

/// File system abstraction for database operations.
///
/// Provides a unified interface for file operations that can be
/// implemented differently for local files, in-memory files, or
/// remote storage.
pub trait FileSystem: Send + Sync + std::fmt::Debug {
    /// Check if a file exists.
    fn file_exists(&self, path: &Path) -> bool;

    /// Check if a directory exists.
    fn directory_exists(&self, path: &Path) -> bool;

    /// Create a directory (and parent directories if needed).
    fn create_directory(&self, path: &Path) -> io::Result<()>;

    /// Remove a file.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Remove a directory (recursively).
    fn remove_directory(&self, path: &Path) -> io::Result<()>;

    /// List files in a directory.
    fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>>;

    /// Get file size.
    fn get_file_size(&self, path: &Path) -> io::Result<u64>;

    /// Read entire file contents.
    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Write entire file contents.
    fn write_file(&self, path: &Path, contents: &[u8]) -> io::Result<()>;

    /// Move/rename a file.
    fn move_file(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Get the canonical path.
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf>;
}

/// Local file system implementation.
///
/// Uses the standard library's file system operations.
#[derive(Debug, Clone)]
pub struct LocalFileSystem;

impl LocalFileSystem {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocalFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystem for LocalFileSystem {
    fn file_exists(&self, path: &Path) -> bool {
        path.exists() && path.is_file()
    }

    fn directory_exists(&self, path: &Path) -> bool {
        path.exists() && path.is_dir()
    }

    fn create_directory(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn remove_directory(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            files.push(entry.path());
        }
        Ok(files)
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        let metadata = fs::metadata(path)?;
        Ok(metadata.len())
    }

    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn write_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        fs::write(path, contents)
    }

    fn move_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        fs::canonicalize(path)
    }
}

#[derive(Debug, Default)]
struct InMemoryFileSystemState {
    files: HashMap<PathBuf, Vec<u8>>,
    directories: HashSet<PathBuf>,
}

/// In-memory file system for testing.
#[derive(Debug, Clone)]
pub struct InMemoryFileSystem {
    state: Arc<Mutex<InMemoryFileSystemState>>,
}

impl InMemoryFileSystem {
    pub fn new() -> Self {
        let mut state = InMemoryFileSystemState::default();
        state.directories.insert(PathBuf::from("/"));
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn normalize_path(path: &Path) -> PathBuf {
        let mut normalized = if path.is_absolute() {
            PathBuf::from("/")
        } else {
            PathBuf::new()
        };

        for component in path.components() {
            match component {
                Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
                Component::RootDir | Component::CurDir => {}
                Component::ParentDir => {
                    normalized.pop();
                }
                Component::Normal(part) => normalized.push(part),
            }
        }

        if normalized.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            normalized
        }
    }

    fn ensure_directory_tree(state: &mut InMemoryFileSystemState, path: &Path) {
        let normalized = Self::normalize_path(path);
        let mut current = if normalized.is_absolute() {
            PathBuf::from("/")
        } else {
            PathBuf::new()
        };

        if normalized.is_absolute() {
            state.directories.insert(current.clone());
        }

        for component in normalized.components() {
            if let Component::Normal(part) = component {
                current.push(part);
                state.directories.insert(current.clone());
            }
        }
    }

    fn not_found(path: &Path) -> io::Error {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("path does not exist: {}", path.display()),
        )
    }
}

impl Default for InMemoryFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystem for InMemoryFileSystem {
    fn file_exists(&self, path: &Path) -> bool {
        let path = Self::normalize_path(path);
        self.state.lock().files.contains_key(&path)
    }

    fn directory_exists(&self, path: &Path) -> bool {
        let path = Self::normalize_path(path);
        self.state.lock().directories.contains(&path)
    }

    fn create_directory(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        Self::ensure_directory_tree(&mut state, path);
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let removed = self.state.lock().files.remove(&path);
        removed.map(|_| ()).ok_or_else(|| Self::not_found(&path))
    }

    fn remove_directory(&self, path: &Path) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let mut state = self.state.lock();
        if !state.directories.contains(&path) {
            return Err(Self::not_found(&path));
        }
        state
            .files
            .retain(|file_path, _| !file_path.starts_with(&path) || file_path == &path);
        state
            .directories
            .retain(|dir_path| !dir_path.starts_with(&path) || dir_path == &PathBuf::from("/"));
        Ok(())
    }

    fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let path = Self::normalize_path(path);
        let state = self.state.lock();
        if !state.directories.contains(&path) {
            return Err(Self::not_found(&path));
        }

        let mut entries = Vec::new();
        for dir in &state.directories {
            if dir != &path && dir.parent() == Some(path.as_path()) {
                entries.push(dir.clone());
            }
        }
        for file in state.files.keys() {
            if file.parent() == Some(path.as_path()) {
                entries.push(file.clone());
            }
        }
        Ok(entries)
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        let path = Self::normalize_path(path);
        self.state
            .lock()
            .files
            .get(&path)
            .map(|contents| contents.len() as u64)
            .ok_or_else(|| Self::not_found(&path))
    }

    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        let path = Self::normalize_path(path);
        self.state
            .lock()
            .files
            .get(&path)
            .cloned()
            .ok_or_else(|| Self::not_found(&path))
    }

    fn write_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let parent = path.parent().unwrap_or_else(|| Path::new("/"));
        let mut state = self.state.lock();
        if !state.directories.contains(parent) {
            return Err(Self::not_found(parent));
        }
        state.files.insert(path, contents.to_vec());
        Ok(())
    }

    fn move_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        let from = Self::normalize_path(from);
        let to = Self::normalize_path(to);
        let parent = to.parent().unwrap_or_else(|| Path::new("/"));
        let mut state = self.state.lock();
        if !state.directories.contains(parent) {
            return Err(Self::not_found(parent));
        }
        let Some(contents) = state.files.remove(&from) else {
            return Err(Self::not_found(&from));
        };
        state.files.insert(to, contents);
        Ok(())
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        Ok(Self::normalize_path(path))
    }
}

/// Database file system wrapper.
///
/// Wraps a FileSystem implementation and provides database-specific
/// file operations.
#[derive(Debug, Clone)]
pub struct DatabaseFileSystem {
    fs: Arc<dyn FileSystem>,
}

impl DatabaseFileSystem {
    /// Create a new DatabaseFileSystem with the given file system.
    pub fn new(fs: Arc<dyn FileSystem>) -> Self {
        Self { fs }
    }

    /// Create a DatabaseFileSystem with the local file system.
    pub fn local() -> Self {
        Self::new(Arc::new(LocalFileSystem::new()))
    }

    /// Create a DatabaseFileSystem with an in-memory file system.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryFileSystem::new()))
    }

    /// Get the underlying file system.
    pub fn file_system(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }

    /// Check if a file exists.
    pub fn file_exists(&self, path: &Path) -> bool {
        self.fs.file_exists(path)
    }

    /// Check if a directory exists.
    pub fn directory_exists(&self, path: &Path) -> bool {
        self.fs.directory_exists(path)
    }

    /// Create a directory.
    pub fn create_directory(&self, path: &Path) -> io::Result<()> {
        self.fs.create_directory(path)
    }

    /// Remove a file.
    pub fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.fs.remove_file(path)
    }

    /// Remove a directory.
    pub fn remove_directory(&self, path: &Path) -> io::Result<()> {
        self.fs.remove_directory(path)
    }

    /// List files in a directory.
    pub fn list_files(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.fs.list_files(path)
    }

    /// Get file size.
    pub fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        self.fs.get_file_size(path)
    }

    /// Read entire file contents.
    pub fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.fs.read_file(path)
    }

    /// Write entire file contents.
    pub fn write_file(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        self.fs.write_file(path, contents)
    }

    /// Move/rename a file.
    pub fn move_file(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.fs.move_file(from, to)
    }

    /// Get the canonical path.
    pub fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.fs.canonicalize(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_local_file_system_basic() {
        let fs = LocalFileSystem::new();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        // File should not exist initially
        assert!(!fs.file_exists(&file_path));

        // Write file
        fs.write_file(&file_path, b"hello world").unwrap();

        // File should exist now
        assert!(fs.file_exists(&file_path));

        // Read file
        let contents = fs.read_file(&file_path).unwrap();
        assert_eq!(contents, b"hello world");

        // Get file size
        let size = fs.get_file_size(&file_path).unwrap();
        assert_eq!(size, 11);

        // Remove file
        fs.remove_file(&file_path).unwrap();
        assert!(!fs.file_exists(&file_path));
    }

    #[test]
    fn test_local_file_system_directory() {
        let fs = LocalFileSystem::new();
        let dir = tempdir().unwrap();
        let sub_dir = dir.path().join("subdir");

        // Directory should not exist initially
        assert!(!fs.directory_exists(&sub_dir));

        // Create directory
        fs.create_directory(&sub_dir).unwrap();

        // Directory should exist now
        assert!(fs.directory_exists(&sub_dir));

        // Remove directory
        fs.remove_directory(&sub_dir).unwrap();
        assert!(!fs.directory_exists(&sub_dir));
    }

    #[test]
    fn test_database_file_system() {
        let db_fs = DatabaseFileSystem::local();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        // Test through DatabaseFileSystem wrapper
        assert!(!db_fs.file_exists(&file_path));
        db_fs.write_file(&file_path, b"test").unwrap();
        assert!(db_fs.file_exists(&file_path));

        let contents = db_fs.read_file(&file_path).unwrap();
        assert_eq!(contents, b"test");
    }

    #[test]
    fn test_in_memory_file_system() {
        let db_fs = DatabaseFileSystem::in_memory();
        let dir = Path::new("/data");
        let path = dir.join("test.txt");
        let moved = dir.join("moved.txt");

        db_fs.create_directory(dir).unwrap();
        db_fs.write_file(&path, b"test").unwrap();

        assert!(db_fs.file_exists(&path));
        assert_eq!(db_fs.read_file(&path).unwrap(), b"test");
        assert_eq!(db_fs.get_file_size(&path).unwrap(), 4);

        let mut entries = db_fs.list_files(dir).unwrap();
        entries.sort();
        assert_eq!(entries, vec![path.clone()]);

        db_fs.move_file(&path, &moved).unwrap();
        assert!(!db_fs.file_exists(&path));
        assert!(db_fs.file_exists(&moved));

        db_fs.remove_file(&moved).unwrap();
        assert!(!db_fs.file_exists(&moved));
    }
}
