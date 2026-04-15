// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! A generic cache for storing objects by key.

use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for objects that can be stored in the ObjectCache.
pub trait ObjectCacheEntry: Send + Sync + Any {
    /// Get the type name of this cache entry.
    fn object_type(&self) -> &'static str;

    /// Convert to Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to Any for downcasting (Arc version).
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

/// A generic cache for storing objects by key.
///
/// It provides a simple key-value store for caching objects
/// that can be shared across the database instance.
///
/// Common use cases:
/// - Caching parsed SQL statements
/// - Caching compiled expressions
/// - Caching metadata lookups
pub struct ObjectCache {
    /// The cache storage.
    cache: Mutex<HashMap<String, Arc<dyn ObjectCacheEntry>>>,
}

impl Default for ObjectCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectCache {
    /// Create a new empty ObjectCache.
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Get an object from the cache by key.
    pub fn get(&self, key: &str) -> Option<Arc<dyn ObjectCacheEntry>> {
        let cache = self.cache.lock();
        cache.get(key).cloned()
    }

    /// Get an object from the cache and downcast to the expected type.
    ///
    /// Returns None if the key doesn't exist or the type doesn't match.
    pub fn get_typed<T: ObjectCacheEntry + 'static>(&self, key: &str) -> Option<Arc<T>> {
        let cache = self.cache.lock();
        cache.get(key).and_then(|entry| {
            // Check if the type matches
            let any = entry.as_any();
            if any.is::<T>() {
                // Clone the Arc and downcast
                let cloned = entry.clone();
                // Use Arc::downcast which is available for Arc<dyn Any>
                let any_arc = cloned.as_any_arc();
                any_arc.downcast::<T>().ok()
            } else {
                None
            }
        })
    }

    /// Get an object from the cache, or create it if it doesn't exist.
    ///
    /// The factory function is only called if the key doesn't exist.
    pub fn get_or_create<T, F>(&self, key: &str, factory: F) -> Arc<T>
    where
        T: ObjectCacheEntry + 'static,
        F: FnOnce() -> T,
    {
        let mut cache = self.cache.lock();

        // Check if entry exists and has correct type
        if let Some(entry) = cache.get(key) {
            let any = entry.as_any();
            if any.is::<T>() {
                let cloned = entry.clone();
                let any_arc = cloned.as_any_arc();
                if let Ok(typed) = any_arc.downcast::<T>() {
                    return typed;
                }
            }
        }

        // Create new entry
        let value = Arc::new(factory());
        cache.insert(key.to_string(), value.clone() as Arc<dyn ObjectCacheEntry>);
        value
    }

    /// Put an object into the cache.
    ///
    /// If an entry with the same key exists, it will be replaced.
    pub fn put<T: ObjectCacheEntry + 'static>(&self, key: String, value: Arc<T>) {
        let mut cache = self.cache.lock();
        cache.insert(key, value as Arc<dyn ObjectCacheEntry>);
    }

    /// Put an object into the cache (dyn version).
    pub fn put_entry(&self, key: String, value: Arc<dyn ObjectCacheEntry>) {
        let mut cache = self.cache.lock();
        cache.insert(key, value);
    }

    /// Delete an object from the cache.
    ///
    /// Returns true if an entry was removed.
    pub fn delete(&self, key: &str) -> bool {
        let mut cache = self.cache.lock();
        cache.remove(key).is_some()
    }

    /// Check if a key exists in the cache.
    pub fn contains(&self, key: &str) -> bool {
        let cache = self.cache.lock();
        cache.contains_key(key)
    }

    /// Get the number of entries in the cache.
    pub fn len(&self) -> usize {
        let cache = self.cache.lock();
        cache.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
    }

    /// Get all keys in the cache.
    pub fn keys(&self) -> Vec<String> {
        let cache = self.cache.lock();
        cache.keys().cloned().collect()
    }
}

impl std::fmt::Debug for ObjectCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache = self.cache.lock();
        f.debug_struct("ObjectCache")
            .field("entry_count", &cache.len())
            .field("keys", &cache.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCacheEntry {
        value: i32,
    }

    impl ObjectCacheEntry for TestCacheEntry {
        fn object_type(&self) -> &'static str {
            "TestCacheEntry"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self
        }
    }

    struct AnotherCacheEntry;

    impl ObjectCacheEntry for AnotherCacheEntry {
        fn object_type(&self) -> &'static str {
            "AnotherCacheEntry"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self
        }
    }

    #[test]
    fn test_object_cache_basic() {
        let cache = ObjectCache::new();
        assert!(cache.is_empty());

        let entry = Arc::new(TestCacheEntry { value: 42 });
        cache.put("test".to_string(), entry);

        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
        assert!(cache.contains("test"));
    }

    #[test]
    fn test_object_cache_get_typed() {
        let cache = ObjectCache::new();

        let entry = Arc::new(TestCacheEntry { value: 42 });
        cache.put("test".to_string(), entry);

        // Get with correct type
        let retrieved = cache.get_typed::<TestCacheEntry>("test");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value, 42);

        // Get with wrong type
        let wrong_type = cache.get_typed::<AnotherCacheEntry>("test");
        assert!(wrong_type.is_none());

        // Get non-existent key
        let missing = cache.get_typed::<TestCacheEntry>("missing");
        assert!(missing.is_none());
    }

    #[test]
    fn test_object_cache_get_or_create() {
        let cache = ObjectCache::new();

        // First call creates the entry
        let entry1 = cache.get_or_create("test", || TestCacheEntry { value: 42 });
        assert_eq!(entry1.value, 42);

        // Second call returns existing entry
        let entry2 = cache.get_or_create("test", || TestCacheEntry { value: 100 });
        assert_eq!(entry2.value, 42); // Still 42, not 100

        // Same Arc
        assert!(Arc::ptr_eq(&entry1, &entry2));
    }

    #[test]
    fn test_object_cache_delete() {
        let cache = ObjectCache::new();

        let entry = Arc::new(TestCacheEntry { value: 42 });
        cache.put("test".to_string(), entry);

        assert!(cache.delete("test"));
        assert!(!cache.contains("test"));
        assert!(!cache.delete("test")); // Already deleted
    }

    #[test]
    fn test_object_cache_clear() {
        let cache = ObjectCache::new();

        cache.put("a".to_string(), Arc::new(TestCacheEntry { value: 1 }));
        cache.put("b".to_string(), Arc::new(TestCacheEntry { value: 2 }));
        cache.put("c".to_string(), Arc::new(TestCacheEntry { value: 3 }));

        assert_eq!(cache.len(), 3);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_object_cache_keys() {
        let cache = ObjectCache::new();

        cache.put("a".to_string(), Arc::new(TestCacheEntry { value: 1 }));
        cache.put("b".to_string(), Arc::new(TestCacheEntry { value: 2 }));

        let mut keys = cache.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn test_object_cache_replace() {
        let cache = ObjectCache::new();

        cache.put("test".to_string(), Arc::new(TestCacheEntry { value: 1 }));
        cache.put("test".to_string(), Arc::new(TestCacheEntry { value: 2 }));

        let entry = cache.get_typed::<TestCacheEntry>("test").unwrap();
        assert_eq!(entry.value, 2);
    }
}
