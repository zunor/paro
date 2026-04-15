//! Global and per-producer task error collection for shared schedulers.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Error data captured from a task execution.
#[derive(Debug, Clone)]
pub struct TaskError {
    /// Error message
    pub message: String,
    /// Optional source error
    pub source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl TaskError {
    /// Create a new task error from a message.
    pub fn new(message: String) -> Self {
        Self {
            message,
            source: None,
        }
    }

    /// Create a new task error from a Paro error.
    pub fn from_paro_error(error: paro_common::error::ParoError) -> Self {
        Self {
            message: error.to_string(),
            source: None,
        }
    }

    /// Create a new task error from a panic payload.
    pub fn from_panic(payload: Box<dyn std::any::Any + Send>) -> Self {
        let message = if let Some(s) = payload.downcast_ref::<&str>() {
            format!("Task panicked: {}", s)
        } else if let Some(s) = payload.downcast_ref::<String>() {
            format!("Task panicked: {}", s)
        } else {
            "Task panicked with unknown payload".to_string()
        };

        Self {
            message,
            source: None,
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Manages errors that occur during parallel task execution.
///
/// This is thread-safe and allows multiple tasks to report errors concurrently.
/// The first error is preserved and can be retrieved later.
pub struct TaskErrorManager {
    /// Lock-free error flag for fast checking
    has_error: AtomicBool,
    /// Mutex-protected error storage
    errors: Mutex<Vec<TaskError>>,
}

impl TaskErrorManager {
    /// Create a new error manager.
    pub fn new() -> Self {
        Self {
            has_error: AtomicBool::new(false),
            errors: Mutex::new(Vec::new()),
        }
    }

    /// Push an error onto the error stack.
    ///
    /// This is thread-safe and can be called from multiple tasks concurrently.
    pub fn push_error(&self, error: TaskError) {
        let mut errors = self.errors.lock();
        errors.push(error);
        self.has_error.store(true, Ordering::SeqCst);
    }

    /// Check if any errors have occurred (lock-free).
    pub fn has_error(&self) -> bool {
        self.has_error.load(Ordering::SeqCst)
    }

    /// Get the first error that occurred.
    ///
    /// Returns None if no errors have occurred.
    pub fn get_error(&self) -> Option<TaskError> {
        let errors = self.errors.lock();
        errors.first().cloned()
    }

    /// Get all errors that occurred.
    pub fn get_all_errors(&self) -> Vec<TaskError> {
        self.errors.lock().clone()
    }

    /// Reset the error manager, clearing all errors.
    pub fn reset(&self) {
        let mut errors = self.errors.lock();
        errors.clear();
        self.has_error.store(false, Ordering::SeqCst);
    }

    /// Get the number of errors.
    pub fn error_count(&self) -> usize {
        self.errors.lock().len()
    }
}

impl Default for TaskErrorManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks task errors globally and per producer.
///
/// Producer-scoped errors are isolated so one query can fail without poisoning
/// unrelated work that shares the same scheduler.
pub struct TaskErrorRegistry {
    global: TaskErrorManager,
    producer_errors: Mutex<HashMap<usize, Arc<TaskErrorManager>>>,
}

impl TaskErrorRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            global: TaskErrorManager::new(),
            producer_errors: Mutex::new(HashMap::new()),
        }
    }

    fn producer_manager(&self, producer_id: usize) -> Arc<TaskErrorManager> {
        let mut producer_errors = self.producer_errors.lock();
        producer_errors
            .entry(producer_id)
            .or_insert_with(|| Arc::new(TaskErrorManager::new()))
            .clone()
    }

    /// Record a global, non-producer-scoped error.
    pub fn push_global_error(&self, error: TaskError) {
        self.global.push_error(error);
    }

    /// Record an error for a specific producer.
    pub fn push_producer_error(&self, producer_id: usize, error: TaskError) {
        self.producer_manager(producer_id).push_error(error);
    }

    /// Check whether any global or producer-scoped error exists.
    pub fn has_any_error(&self) -> bool {
        if self.global.has_error() {
            return true;
        }

        self.producer_errors
            .lock()
            .values()
            .any(|manager| manager.has_error())
    }

    /// Check whether a global, non-producer-scoped error exists.
    pub fn has_global_error(&self) -> bool {
        self.global.has_error()
    }

    /// Check whether a specific producer has an error.
    pub fn has_error_for_producer(&self, producer_id: usize) -> bool {
        self.producer_errors
            .lock()
            .get(&producer_id)
            .is_some_and(|manager| manager.has_error())
    }

    /// Get the first global error or, if none exists, the first producer error.
    pub fn get_any_error(&self) -> Option<TaskError> {
        if let Some(error) = self.global.get_error() {
            return Some(error);
        }

        self.producer_errors
            .lock()
            .values()
            .find_map(|manager| manager.get_error())
    }

    /// Get every tracked error.
    pub fn get_all_errors(&self) -> Vec<TaskError> {
        let mut errors = self.global.get_all_errors();
        let producer_errors = self.producer_errors.lock();
        for manager in producer_errors.values() {
            errors.extend(manager.get_all_errors());
        }
        errors
    }

    /// Get the first global error.
    pub fn get_global_error(&self) -> Option<TaskError> {
        self.global.get_error()
    }

    /// Get the first error for a producer.
    pub fn get_error_for_producer(&self, producer_id: usize) -> Option<TaskError> {
        self.producer_errors
            .lock()
            .get(&producer_id)
            .and_then(|manager| manager.get_error())
    }

    /// Get every tracked error for a producer.
    pub fn get_all_errors_for_producer(&self, producer_id: usize) -> Vec<TaskError> {
        self.producer_errors
            .lock()
            .get(&producer_id)
            .map(|manager| manager.get_all_errors())
            .unwrap_or_default()
    }

    /// Clear every tracked error.
    pub fn reset_all(&self) {
        self.global.reset();
        self.producer_errors.lock().clear();
    }

    /// Clear the errors associated with a single producer.
    pub fn reset_producer(&self, producer_id: usize) {
        self.producer_errors.lock().remove(&producer_id);
    }
}

impl Default for TaskErrorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_manager_basic() {
        let manager = TaskErrorManager::new();
        assert!(!manager.has_error());
        assert_eq!(manager.error_count(), 0);

        manager.push_error(TaskError::new("Test error".to_string()));
        assert!(manager.has_error());
        assert_eq!(manager.error_count(), 1);

        let error = manager.get_error().unwrap();
        assert_eq!(error.message, "Test error");
    }

    #[test]
    fn test_error_manager_multiple_errors() {
        let manager = TaskErrorManager::new();

        manager.push_error(TaskError::new("Error 1".to_string()));
        manager.push_error(TaskError::new("Error 2".to_string()));
        manager.push_error(TaskError::new("Error 3".to_string()));

        assert_eq!(manager.error_count(), 3);

        // First error should be returned
        let error = manager.get_error().unwrap();
        assert_eq!(error.message, "Error 1");

        // All errors should be available
        let all_errors = manager.get_all_errors();
        assert_eq!(all_errors.len(), 3);
        assert_eq!(all_errors[0].message, "Error 1");
        assert_eq!(all_errors[1].message, "Error 2");
        assert_eq!(all_errors[2].message, "Error 3");
    }

    #[test]
    fn test_error_manager_reset() {
        let manager = TaskErrorManager::new();

        manager.push_error(TaskError::new("Test error".to_string()));
        assert!(manager.has_error());

        manager.reset();
        assert!(!manager.has_error());
        assert_eq!(manager.error_count(), 0);
        assert!(manager.get_error().is_none());
    }

    #[test]
    fn test_error_manager_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let manager = Arc::new(TaskErrorManager::new());
        let mut handles = vec![];

        // Spawn multiple threads that push errors
        for i in 0..10 {
            let manager_clone = manager.clone();
            handles.push(thread::spawn(move || {
                manager_clone.push_error(TaskError::new(format!("Error {}", i)));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(manager.has_error());
        assert_eq!(manager.error_count(), 10);
    }

    #[test]
    fn test_task_error_from_panic() {
        let error = TaskError::from_panic(Box::new("panic message"));
        assert!(error.message.contains("panic message"));

        let error = TaskError::from_panic(Box::new("another panic".to_string()));
        assert!(error.message.contains("another panic"));

        let error = TaskError::from_panic(Box::new(42));
        assert!(error.message.contains("unknown payload"));
    }

    #[test]
    fn test_error_registry_keeps_producer_errors_isolated() {
        let registry = TaskErrorRegistry::new();

        registry.push_producer_error(1, TaskError::new("producer 1 failed".to_string()));

        assert!(registry.has_any_error());
        assert!(registry.has_error_for_producer(1));
        assert!(!registry.has_error_for_producer(2));
        assert_eq!(
            registry
                .get_error_for_producer(1)
                .expect("producer error")
                .message,
            "producer 1 failed"
        );
        assert!(registry.get_error_for_producer(2).is_none());
    }

    #[test]
    fn test_error_registry_reset_producer() {
        let registry = TaskErrorRegistry::new();

        registry.push_producer_error(7, TaskError::new("producer 7 failed".to_string()));
        registry.reset_producer(7);

        assert!(!registry.has_error_for_producer(7));
        assert!(registry.get_error_for_producer(7).is_none());
    }
}
