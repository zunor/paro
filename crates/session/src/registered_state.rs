// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Extensible session-local state shared across query and transaction lifecycles.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use paro_common::error::ParoError;

// ============================================================================
// SessionContextState trait
// ============================================================================

/// Trait for extensible session-local state.
///
/// `SessionContextState` provides a plugin mechanism for registering
/// arbitrary state that lives as long as a Session. It supports lifecycle
/// callbacks for queries and transactions.
///
///
/// # Use Cases
/// - Caches that need cleanup at query end
/// - State for tracking query/transaction lifecycle
/// - Extension-specific private state
///
/// # Example
/// ```ignore
/// struct MyCache {
///     data: HashMap<String, String>,
/// }
///
/// impl SessionContextState for MyCache {
///     fn query_end(&mut self, error: Option<&ParoError>) {
///         // Clear cache on query end
///         self.data.clear();
///     }
/// }
/// ```
pub trait SessionContextState: Send + Sync + Any + std::fmt::Debug {
    /// Called when a query begins.
    fn query_begin(&mut self) {}

    /// Called when a query ends.
    ///
    /// # Arguments
    /// * `error` - The error if the query failed, None if successful
    fn query_end(&mut self, _error: Option<&ParoError>) {}

    /// Called when a transaction begins.
    fn transaction_begin(&mut self) {}

    /// Called when a transaction commits.
    fn transaction_commit(&mut self) {}

    /// Called when a transaction rolls back.
    ///
    /// # Arguments
    /// * `error` - The error that caused the rollback, if any
    fn transaction_rollback(&mut self, _error: Option<&ParoError>) {}

    /// Returns self as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns self as mutable Any for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// ============================================================================
// RegisteredStateManager
// ============================================================================

/// Manager for registered session states.
///
/// Provides a key-value store for `SessionContextState` implementations,
/// allowing dynamic registration and retrieval of extensible state.
///
/// # Thread Safety
/// All operations are protected by a mutex for thread-safe access.
///
///
/// # Example
/// ```ignore
/// let mut manager = RegisteredStateManager::new();
///
/// // Register a new state
/// manager.insert("my_cache", Arc::new(Mutex::new(MyCache::new())));
///
/// // Get or create state
/// let cache = manager.get_or_create::<MyCache>("my_cache");
/// ```
#[derive(Debug, Default)]
pub struct RegisteredStateManager {
    /// Registered states, protected by mutex
    states: Mutex<HashMap<String, Arc<Mutex<dyn SessionContextState>>>>,
}

impl RegisteredStateManager {
    /// Creates a new empty state manager.
    pub fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Gets an existing state or creates a new one using Default.
    ///
    /// # Type Parameters
    /// * `T` - The state type, must implement SessionContextState + Default
    ///
    /// # Arguments
    /// * `key` - The key to identify the state
    ///
    /// # Returns
    /// The state wrapped in Arc<Mutex<dyn SessionContextState>>
    pub fn get_or_create<T>(&self, key: &str) -> Arc<Mutex<dyn SessionContextState>>
    where
        T: SessionContextState + Default + 'static,
    {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(state) = states.get(key) {
            return state.clone();
        }

        // Create new state
        let new_state: Arc<Mutex<dyn SessionContextState>> = Arc::new(Mutex::new(T::default()));
        states.insert(key.to_string(), new_state.clone());
        new_state
    }

    /// Gets an existing state by key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// The state if found, None otherwise
    pub fn get(&self, key: &str) -> Option<Arc<Mutex<dyn SessionContextState>>> {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        states.get(key).cloned()
    }

    /// Checks if a state exists and is of the expected type.
    pub fn contains<T: 'static>(&self, key: &str) -> bool {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = states.get(key) {
            if let Ok(guard) = state.lock() {
                return guard.as_any().is::<T>();
            }
        }
        false
    }

    /// Inserts a state with the given key.
    ///
    /// # Arguments
    /// * `key` - The key to identify the state
    /// * `state` - The state to insert
    pub fn insert<T>(&self, key: &str, state: T)
    where
        T: SessionContextState + 'static,
    {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let dyn_state: Arc<Mutex<dyn SessionContextState>> = Arc::new(Mutex::new(state));
        states.insert(key.to_string(), dyn_state);
    }

    /// Removes a state by key.
    ///
    /// # Arguments
    /// * `key` - The key to remove
    ///
    /// # Returns
    /// true if the state was removed, false if not found
    pub fn remove(&self, key: &str) -> bool {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        states.remove(key).is_some()
    }

    /// Returns all registered states.
    pub fn states(&self) -> Vec<Arc<Mutex<dyn SessionContextState>>> {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        states.values().cloned().collect()
    }

    /// Returns the number of registered states.
    pub fn len(&self) -> usize {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        states.len()
    }

    /// Returns true if no states are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Notifies all states that a query has begun.
    pub fn notify_query_begin(&self) {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        for state in states.values() {
            if let Ok(mut guard) = state.lock() {
                guard.query_begin();
            }
        }
    }

    /// Notifies all states that a query has ended.
    pub fn notify_query_end(&self, error: Option<&ParoError>) {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        for state in states.values() {
            if let Ok(mut guard) = state.lock() {
                guard.query_end(error);
            }
        }
    }

    /// Notifies all states that a transaction has begun.
    pub fn notify_transaction_begin(&self) {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        for state in states.values() {
            if let Ok(mut guard) = state.lock() {
                guard.transaction_begin();
            }
        }
    }

    /// Notifies all states that a transaction has committed.
    pub fn notify_transaction_commit(&self) {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        for state in states.values() {
            if let Ok(mut guard) = state.lock() {
                guard.transaction_commit();
            }
        }
    }

    /// Notifies all states that a transaction has rolled back.
    pub fn notify_transaction_rollback(&self, error: Option<&ParoError>) {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        for state in states.values() {
            if let Ok(mut guard) = state.lock() {
                guard.transaction_rollback(error);
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    // Test state implementation
    #[derive(Debug, Default)]
    struct TestState {
        query_begin_count: AtomicU32,
        query_end_count: AtomicU32,
        transaction_begin_count: AtomicU32,
        transaction_commit_count: AtomicU32,
        transaction_rollback_count: AtomicU32,
    }

    impl SessionContextState for TestState {
        fn query_begin(&mut self) {
            self.query_begin_count.fetch_add(1, Ordering::Relaxed);
        }

        fn query_end(&mut self, _error: Option<&ParoError>) {
            self.query_end_count.fetch_add(1, Ordering::Relaxed);
        }

        fn transaction_begin(&mut self) {
            self.transaction_begin_count.fetch_add(1, Ordering::Relaxed);
        }

        fn transaction_commit(&mut self) {
            self.transaction_commit_count
                .fetch_add(1, Ordering::Relaxed);
        }

        fn transaction_rollback(&mut self, _error: Option<&ParoError>) {
            self.transaction_rollback_count
                .fetch_add(1, Ordering::Relaxed);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[derive(Debug, Default)]
    struct AnotherState;

    impl SessionContextState for AnotherState {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn test_registered_state_manager_new() {
        let manager = RegisteredStateManager::new();
        assert!(manager.is_empty());
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_registered_state_manager_insert_and_get() {
        let manager = RegisteredStateManager::new();

        manager.insert("test", TestState::default());
        assert_eq!(manager.len(), 1);

        let state = manager.get("test");
        assert!(state.is_some());

        // Check type
        assert!(manager.contains::<TestState>("test"));
        assert!(!manager.contains::<AnotherState>("test"));
    }

    #[test]
    fn test_registered_state_manager_get_or_create() {
        let manager = RegisteredStateManager::new();

        // First call creates
        let state1 = manager.get_or_create::<TestState>("test");
        assert_eq!(manager.len(), 1);

        // Second call returns existing
        let _state2 = manager.get_or_create::<TestState>("test");
        assert_eq!(manager.len(), 1);

        // Modify through state1
        {
            let mut guard = state1.lock().unwrap();
            guard.query_begin();
        }

        // Verify modification persists
        let state3 = manager.get("test").unwrap();
        {
            let guard = state3.lock().unwrap();
            let test_state = guard.as_any().downcast_ref::<TestState>().unwrap();
            assert_eq!(test_state.query_begin_count.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn test_registered_state_manager_remove() {
        let manager = RegisteredStateManager::new();

        manager.insert("test", TestState::default());
        assert_eq!(manager.len(), 1);

        assert!(manager.remove("test"));
        assert!(manager.is_empty());

        // Remove non-existent
        assert!(!manager.remove("nonexistent"));
    }

    #[test]
    fn test_registered_state_manager_notify_query() {
        let manager = RegisteredStateManager::new();
        manager.insert("test", TestState::default());

        manager.notify_query_begin();
        manager.notify_query_end(None);

        let state = manager.get("test").unwrap();
        let guard = state.lock().unwrap();
        let test_state = guard.as_any().downcast_ref::<TestState>().unwrap();
        assert_eq!(test_state.query_begin_count.load(Ordering::Relaxed), 1);
        assert_eq!(test_state.query_end_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_registered_state_manager_notify_transaction() {
        let manager = RegisteredStateManager::new();
        manager.insert("test", TestState::default());

        manager.notify_transaction_begin();
        manager.notify_transaction_commit();

        let state = manager.get("test").unwrap();
        let guard = state.lock().unwrap();
        let test_state = guard.as_any().downcast_ref::<TestState>().unwrap();
        assert_eq!(
            test_state.transaction_begin_count.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            test_state.transaction_commit_count.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_registered_state_manager_notify_rollback() {
        let manager = RegisteredStateManager::new();
        manager.insert("test", TestState::default());

        manager.notify_transaction_begin();
        manager.notify_transaction_rollback(None);

        let state = manager.get("test").unwrap();
        let guard = state.lock().unwrap();
        let test_state = guard.as_any().downcast_ref::<TestState>().unwrap();
        assert_eq!(
            test_state.transaction_begin_count.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            test_state
                .transaction_rollback_count
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_registered_state_manager_states() {
        let manager = RegisteredStateManager::new();
        manager.insert("test1", TestState::default());
        manager.insert("test2", AnotherState::default());

        let states = manager.states();
        assert_eq!(states.len(), 2);
    }
}
