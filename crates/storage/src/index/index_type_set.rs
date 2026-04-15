//! # Index Type Set
//!
//! Registry for index types.
//!
//! ## Design Notes
//!
//! The `IndexTypeSet` is a thread-safe registry for index types.
//! It is initialized with callback-driven build types and allows
//! registering additional index types at runtime.

use std::collections::HashMap;
use std::sync::RwLock;

use paro_common::error::{self as paro_error, Result};

use super::index_builder::{get_bitmap_index_type, get_bloom_index_type};
use super::IndexType;

/// Registry for index types.
///
/// This is a thread-safe registry that stores all available index types.
/// By default, it contains callback-driven columnar build types.
///
/// # Example
///
/// ```ignore
/// let set = IndexTypeSet::new();
///
/// // Find an index type
/// if let Some(bloom) = set.find_by_name("BLOOM") {
///     assert_eq!(bloom.name(), "BLOOM");
/// }
///
/// // Register a custom index type
/// set.register_index_type(my_custom_index_type)?;
/// ```
pub struct IndexTypeSet {
    /// Map of index type name to IndexType
    types: RwLock<HashMap<String, IndexType>>,
}

impl IndexTypeSet {
    /// Creates a new IndexTypeSet with default index types.
    ///
    /// Currently registers:
    /// - BLOOM (Bloom filter) with build callbacks
    /// - BITMAP (Bitmap index) with build callbacks
    pub fn new() -> Self {
        let mut types = HashMap::new();

        // Register Bloom Filter index
        types.insert("BLOOM".to_string(), get_bloom_index_type());
        // Register Bitmap index
        types.insert("BITMAP".to_string(), get_bitmap_index_type());

        Self {
            types: RwLock::new(types),
        }
    }

    /// Creates an empty IndexTypeSet without any default types.
    ///
    /// This is useful for testing or when you want full control
    /// over which index types are available.
    pub fn empty() -> Self {
        Self {
            types: RwLock::new(HashMap::new()),
        }
    }

    /// Finds an index type by name.
    ///
    /// # Arguments
    /// * `name` - The name of the index type (case-sensitive)
    ///
    /// # Returns
    /// A clone of the IndexType if found, None otherwise
    pub fn find_by_name(&self, name: &str) -> Option<IndexType> {
        let types = self.types.read().ok()?;
        types.get(name).map(|t| IndexType {
            name: t.name.clone(),
            build_bind: t.build_bind,
            build_sort: t.build_sort,
            build_global_init: t.build_global_init,
            build_local_init: t.build_local_init,
            build_sink: t.build_sink,
            build_combine: t.build_combine,
            build_finalize: t.build_finalize,
            create_plan: t.create_plan,
            create_instance: t.create_instance,
            index_info: t.index_info.clone(),
        })
    }

    /// Checks if an index type with the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.types
            .read()
            .map(|types| types.contains_key(name))
            .unwrap_or(false)
    }

    /// Registers a new index type.
    ///
    /// # Arguments
    /// * `index_type` - The index type to register
    ///
    /// # Errors
    /// Returns an error if an index type with the same name already exists.
    pub fn register_index_type(&self, index_type: IndexType) -> Result<()> {
        let mut types = self
            .types
            .write()
            .map_err(|_| paro_error::internal("Failed to acquire write lock on index types"))?;

        if types.contains_key(&index_type.name) {
            return Err(paro_error::object_exists("index type", &index_type.name));
        }

        types.insert(index_type.name.clone(), index_type);
        Ok(())
    }

    /// Unregisters an index type by name.
    ///
    /// # Arguments
    /// * `name` - The name of the index type to remove
    ///
    /// # Returns
    /// The removed IndexType if it existed, None otherwise
    pub fn unregister_index_type(&self, name: &str) -> Option<IndexType> {
        let mut types = self.types.write().ok()?;
        types.remove(name)
    }

    /// Returns a list of all registered index type names.
    pub fn list_types(&self) -> Vec<String> {
        self.types
            .read()
            .map(|types| types.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns the number of registered index types.
    pub fn len(&self) -> usize {
        self.types.read().map(|types| types.len()).unwrap_or(0)
    }

    /// Returns true if no index types are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for IndexTypeSet {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for IndexTypeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let types = self.list_types();
        f.debug_struct("IndexTypeSet")
            .field("types", &types)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_has_default_build_types() {
        let set = IndexTypeSet::new();
        assert!(set.contains("BLOOM"));
        assert!(set.contains("BITMAP"));
        assert!(!set.is_empty());
    }

    #[test]
    fn test_empty() {
        let set = IndexTypeSet::empty();
        assert!(set.is_empty());
        assert!(!set.contains("BLOOM"));
        assert!(!set.contains("BITMAP"));
    }

    #[test]
    fn test_find_by_name() {
        let set = IndexTypeSet::new();

        let bloom = set.find_by_name("BLOOM");
        assert!(bloom.is_some());
        assert_eq!(bloom.unwrap().name, "BLOOM");

        let nonexistent = set.find_by_name("NONEXISTENT");
        assert!(nonexistent.is_none());
    }

    #[test]
    fn test_register_index_type() {
        let set = IndexTypeSet::empty();

        let custom_type = IndexType::new("CUSTOM");
        assert!(set.register_index_type(custom_type).is_ok());
        assert!(set.contains("CUSTOM"));

        // Try to register again - should fail
        let duplicate = IndexType::new("CUSTOM");
        assert!(set.register_index_type(duplicate).is_err());
    }

    #[test]
    fn test_unregister_index_type() {
        let set = IndexTypeSet::new();
        assert!(set.contains("BLOOM"));

        let removed = set.unregister_index_type("BLOOM");
        assert!(removed.is_some());
        assert!(!set.contains("BLOOM"));

        // Try to remove again - should return None
        let removed_again = set.unregister_index_type("BLOOM");
        assert!(removed_again.is_none());
    }

    #[test]
    fn test_list_types() {
        let set = IndexTypeSet::empty();
        assert!(set.list_types().is_empty());

        set.register_index_type(IndexType::new("TYPE_A")).unwrap();
        set.register_index_type(IndexType::new("TYPE_B")).unwrap();

        let types = set.list_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&"TYPE_A".to_string()));
        assert!(types.contains(&"TYPE_B".to_string()));
    }

    #[test]
    fn test_len() {
        let set = IndexTypeSet::empty();
        assert_eq!(set.len(), 0);

        set.register_index_type(IndexType::new("A")).unwrap();
        assert_eq!(set.len(), 1);

        set.register_index_type(IndexType::new("B")).unwrap();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let set = Arc::new(IndexTypeSet::empty());
        let mut handles = vec![];

        // Spawn multiple threads to register index types
        for i in 0..10 {
            let set_clone = Arc::clone(&set);
            let handle = thread::spawn(move || {
                let name = format!("TYPE_{}", i);
                let _ = set_clone.register_index_type(IndexType::new(&name));
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // All types should be registered
        assert_eq!(set.len(), 10);
    }

    #[test]
    fn test_debug() {
        let set = IndexTypeSet::new();
        let debug_str = format!("{:?}", set);
        assert!(debug_str.contains("IndexTypeSet"));
        assert!(debug_str.contains("BLOOM"));
        assert!(debug_str.contains("BITMAP"));
    }
}
