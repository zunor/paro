// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tracks the validity state of a database instance.

use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Tracks the validity state of a database instance.
///
/// When a fatal error occurs (e.g., storage corruption, out of memory),
/// the database instance is marked as invalid and all subsequent
/// operations will fail with the recorded error message.
///
/// This provides a clean way to handle unrecoverable errors without
/// leaving the database in an inconsistent state.
pub struct ValidChecker {
    /// Lock for invalidation operations.
    invalidate_lock: Mutex<()>,

    /// Whether the database has been invalidated.
    is_invalidated: AtomicBool,

    /// The error message that caused invalidation.
    invalidated_msg: Mutex<String>,
}

impl Default for ValidChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidChecker {
    /// Create a new ValidChecker in valid state.
    pub fn new() -> Self {
        Self {
            invalidate_lock: Mutex::new(()),
            is_invalidated: AtomicBool::new(false),
            invalidated_msg: Mutex::new(String::new()),
        }
    }

    /// Invalidate the database with an error message.
    ///
    /// Once invalidated, the database cannot be un-invalidated.
    /// All subsequent operations should check `is_invalidated()` and
    /// fail appropriately.
    pub fn invalidate(&self, error: String) {
        let _guard = self.invalidate_lock.lock();

        // Only set the message if not already invalidated
        if !self.is_invalidated.load(Ordering::SeqCst) {
            let mut msg = self.invalidated_msg.lock();
            *msg = error;
            self.is_invalidated.store(true, Ordering::SeqCst);
        }
    }

    /// Check if the database has been invalidated.
    pub fn is_invalidated(&self) -> bool {
        self.is_invalidated.load(Ordering::SeqCst)
    }

    /// Get the error message that caused invalidation.
    ///
    /// Returns an empty string if not invalidated.
    pub fn invalidated_message(&self) -> String {
        let msg = self.invalidated_msg.lock();
        msg.clone()
    }

    /// Check validity and return an error if invalidated.
    ///
    /// This is a convenience method for checking validity at the
    /// start of operations.
    pub fn check_valid(&self) -> Result<(), String> {
        if self.is_invalidated() {
            Err(self.invalidated_message())
        } else {
            Ok(())
        }
    }

    /// Reset the validity state (for testing only).
    ///
    /// In production, once invalidated, a database should not be
    /// un-invalidated. This method is provided for testing purposes.
    #[cfg(test)]
    pub fn reset(&self) {
        let _guard = self.invalidate_lock.lock();
        self.is_invalidated.store(false, Ordering::SeqCst);
        let mut msg = self.invalidated_msg.lock();
        msg.clear();
    }
}

impl std::fmt::Debug for ValidChecker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidChecker")
            .field("is_invalidated", &self.is_invalidated())
            .field(
                "message",
                &if self.is_invalidated() {
                    self.invalidated_message()
                } else {
                    "(valid)".to_string()
                },
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_checker_initial_state() {
        let checker = ValidChecker::new();
        assert!(!checker.is_invalidated());
        assert!(checker.invalidated_message().is_empty());
        assert!(checker.check_valid().is_ok());
    }

    #[test]
    fn test_valid_checker_invalidate() {
        let checker = ValidChecker::new();

        checker.invalidate("Storage corruption detected".to_string());

        assert!(checker.is_invalidated());
        assert_eq!(checker.invalidated_message(), "Storage corruption detected");
        assert!(checker.check_valid().is_err());
    }

    #[test]
    fn test_valid_checker_double_invalidate() {
        let checker = ValidChecker::new();

        checker.invalidate("First error".to_string());
        checker.invalidate("Second error".to_string());

        // First error message should be preserved
        assert_eq!(checker.invalidated_message(), "First error");
    }

    #[test]
    fn test_valid_checker_check_valid() {
        let checker = ValidChecker::new();

        // Valid state
        let result = checker.check_valid();
        assert!(result.is_ok());

        // Invalidated state
        checker.invalidate("Test error".to_string());
        let result = checker.check_valid();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Test error");
    }

    #[test]
    fn test_valid_checker_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let checker = Arc::new(ValidChecker::new());
        let mut handles = vec![];

        // Spawn multiple threads trying to invalidate
        for i in 0..10 {
            let checker_clone = checker.clone();
            let handle = thread::spawn(move || {
                checker_clone.invalidate(format!("Error from thread {}", i));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should be invalidated with one of the error messages
        assert!(checker.is_invalidated());
        assert!(checker
            .invalidated_message()
            .starts_with("Error from thread"));
    }

    #[test]
    fn test_valid_checker_reset() {
        let checker = ValidChecker::new();

        checker.invalidate("Test error".to_string());
        assert!(checker.is_invalidated());

        checker.reset();
        assert!(!checker.is_invalidated());
        assert!(checker.invalidated_message().is_empty());
    }
}
