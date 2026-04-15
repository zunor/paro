//! # IndexSet - Table Index Collection
//!
//! Manages a collection of indexes attached to a table.
//!
//! ## Design Notes
//!
//! This is a simplified index list. It provides:
//! - Thread-safe index storage
//! - Index lookup by name
//! - Iteration over indexes
//!
//! Index binding is handled at a higher level (Catalog).

use std::sync::{Arc, RwLock};

use paro_common::error::Result;

use crate::index::BoundIndex;

/// A collection of indexes attached to a table.
///
/// This structure manages the indexes for a single table, providing
/// thread-safe access for concurrent operations.
#[derive(Default)]
pub(crate) struct IndexSet {
    /// The indexes in this set, protected by a read-write lock.
    indexes: RwLock<Vec<Arc<dyn BoundIndex>>>,
}

impl std::fmt::Debug for IndexSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let indexes = self.indexes.read().unwrap();
        f.debug_struct("IndexSet")
            .field("count", &indexes.len())
            .finish()
    }
}

impl IndexSet {
    /// Creates a new empty IndexSet.
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(Vec::new()),
        }
    }

    /// Adds an index to the set.
    ///
    /// # Arguments
    /// * `index` - The bound index to add
    ///
    /// # Returns
    /// Ok(()) on success, or an error if an index with the same name exists
    pub fn add_index(&self, index: Arc<dyn BoundIndex>) -> Result<()> {
        let mut indexes = self.indexes.write().unwrap();

        // Check for duplicate name
        let name = index.index_name();
        if indexes.iter().any(|idx| idx.index_name() == name) {
            return Err(paro_common::error::catalog(format!(
                "Index '{}' already exists",
                name
            )));
        }

        indexes.push(index);
        Ok(())
    }

    /// Removes an index by name.
    ///
    /// # Arguments
    /// * `name` - The name of the index to remove
    ///
    /// # Returns
    /// The removed index if found, None otherwise
    pub fn remove_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        let mut indexes = self.indexes.write().unwrap();

        if let Some(pos) = indexes.iter().position(|idx| idx.index_name() == name) {
            Some(indexes.remove(pos))
        } else {
            None
        }
    }

    /// Finds an index by name.
    ///
    /// # Arguments
    /// * `name` - The name of the index to find
    ///
    /// # Returns
    /// A reference to the index if found
    pub fn find_by_name(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        let indexes = self.indexes.read().unwrap();
        indexes.iter().find(|idx| idx.index_name() == name).cloned()
    }

    /// Checks if an index with the given name exists.
    pub fn has_index(&self, name: &str) -> bool {
        let indexes = self.indexes.read().unwrap();
        indexes.iter().any(|idx| idx.index_name() == name)
    }

    #[cfg(test)]
    /// Returns true if the set is empty.
    pub fn is_empty(&self) -> bool {
        let indexes = self.indexes.read().unwrap();
        indexes.is_empty()
    }

    /// Returns the number of indexes in the set.
    pub fn len(&self) -> usize {
        let indexes = self.indexes.read().unwrap();
        indexes.len()
    }

    #[cfg(test)]
    /// Scans all indexes, invoking the callback for each.
    ///
    /// The callback returns `true` to continue scanning, `false` to stop.
    ///
    /// # Arguments
    /// * `callback` - Function to call for each index
    pub fn scan<F>(&self, mut callback: F)
    where
        F: FnMut(&dyn BoundIndex) -> bool,
    {
        let indexes = self.indexes.read().unwrap();
        for index in indexes.iter() {
            if !callback(index.as_ref()) {
                break;
            }
        }
    }

    /// Returns all indexes as a vector.
    ///
    /// This creates a snapshot of the current indexes.
    pub fn get_all(&self) -> Vec<Arc<dyn BoundIndex>> {
        let indexes = self.indexes.read().unwrap();
        indexes.clone()
    }

    #[cfg(test)]
    /// Clears all indexes from the set.
    pub fn clear(&self) {
        let mut indexes = self.indexes.write().unwrap();
        indexes.clear();
    }

    #[cfg(test)]
    /// Returns the total in-memory size of all indexes.
    pub fn get_in_memory_size(&self) -> usize {
        let indexes = self.indexes.read().unwrap();
        indexes.iter().map(|idx| idx.get_in_memory_size()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{ColumnId, IndexConstraintType, IndexStorageInfo};
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    /// A mock BoundIndex for testing.
    struct MockIndex {
        name: String,
        index_type: String,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
    }

    impl MockIndex {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                index_type: "MOCK".to_string(),
                constraint_type: IndexConstraintType::None,
                column_ids: vec![0],
                logical_types: vec![LogicalType::Integer],
            }
        }
    }

    impl crate::index::Index for MockIndex {
        fn index_name(&self) -> &str {
            &self.name
        }

        fn index_type(&self) -> &str {
            &self.index_type
        }

        fn constraint_type(&self) -> IndexConstraintType {
            self.constraint_type
        }

        fn column_ids(&self) -> &[ColumnId] {
            &self.column_ids
        }

        fn is_bound(&self) -> bool {
            true
        }

        fn commit_drop(&mut self) -> Result<()> {
            Ok(())
        }
    }

    impl BoundIndex for MockIndex {
        fn physical_types(&self) -> &[LogicalType] {
            &self.logical_types
        }

        fn logical_types(&self) -> &[LogicalType] {
            &self.logical_types
        }

        fn append(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
            Ok(())
        }

        fn delete(&self, _entries: &Chunk, _row_ids: &Vector) -> Result<usize> {
            Ok(0)
        }

        fn insert(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
            Ok(())
        }

        fn merge_indexes(&self, _other: &dyn BoundIndex) -> Result<bool> {
            Ok(true)
        }

        fn vacuum(&self) {}

        fn get_in_memory_size(&self) -> usize {
            1024
        }

        fn serialize_to_disk(&self) -> Result<IndexStorageInfo> {
            Ok(IndexStorageInfo::default())
        }
    }

    #[test]
    fn test_index_set_new() {
        let set = IndexSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_index_set_add_and_find() {
        let set = IndexSet::new();
        let index = Arc::new(MockIndex::new("test_idx")) as Arc<dyn BoundIndex>;

        set.add_index(index).unwrap();

        assert!(!set.is_empty());
        assert_eq!(set.len(), 1);
        assert!(set.has_index("test_idx"));
        assert!(!set.has_index("other_idx"));

        let found = set.find_by_name("test_idx");
        assert!(found.is_some());
        assert_eq!(found.unwrap().index_name(), "test_idx");
    }

    #[test]
    fn test_index_set_duplicate_name() {
        let set = IndexSet::new();
        let index1 = Arc::new(MockIndex::new("test_idx")) as Arc<dyn BoundIndex>;
        let index2 = Arc::new(MockIndex::new("test_idx")) as Arc<dyn BoundIndex>;

        set.add_index(index1).unwrap();
        let result = set.add_index(index2);

        assert!(result.is_err());
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_index_set_remove() {
        let set = IndexSet::new();
        let index = Arc::new(MockIndex::new("test_idx")) as Arc<dyn BoundIndex>;

        set.add_index(index).unwrap();
        assert_eq!(set.len(), 1);

        let removed = set.remove_index("test_idx");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().index_name(), "test_idx");
        assert!(set.is_empty());

        // Remove non-existent
        let removed2 = set.remove_index("test_idx");
        assert!(removed2.is_none());
    }

    #[test]
    fn test_index_set_scan() {
        let set = IndexSet::new();
        set.add_index(Arc::new(MockIndex::new("idx1")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx2")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx3")) as Arc<dyn BoundIndex>)
            .unwrap();

        let mut names = Vec::new();
        set.scan(|idx| {
            names.push(idx.index_name().to_string());
            true
        });

        assert_eq!(names.len(), 3);
        assert!(names.contains(&"idx1".to_string()));
        assert!(names.contains(&"idx2".to_string()));
        assert!(names.contains(&"idx3".to_string()));
    }

    #[test]
    fn test_index_set_scan_early_stop() {
        let set = IndexSet::new();
        set.add_index(Arc::new(MockIndex::new("idx1")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx2")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx3")) as Arc<dyn BoundIndex>)
            .unwrap();

        let mut count = 0;
        set.scan(|_idx| {
            count += 1;
            count < 2 // Stop after 2
        });

        assert_eq!(count, 2);
    }

    #[test]
    fn test_index_set_get_all() {
        let set = IndexSet::new();
        set.add_index(Arc::new(MockIndex::new("idx1")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx2")) as Arc<dyn BoundIndex>)
            .unwrap();

        let all = set.get_all();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_index_set_clear() {
        let set = IndexSet::new();
        set.add_index(Arc::new(MockIndex::new("idx1")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx2")) as Arc<dyn BoundIndex>)
            .unwrap();

        assert_eq!(set.len(), 2);
        set.clear();
        assert!(set.is_empty());
    }

    #[test]
    fn test_index_set_in_memory_size() {
        let set = IndexSet::new();
        set.add_index(Arc::new(MockIndex::new("idx1")) as Arc<dyn BoundIndex>)
            .unwrap();
        set.add_index(Arc::new(MockIndex::new("idx2")) as Arc<dyn BoundIndex>)
            .unwrap();

        // Each MockIndex returns 1024 bytes
        assert_eq!(set.get_in_memory_size(), 2048);
    }
}
