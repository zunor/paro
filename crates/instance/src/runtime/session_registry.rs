// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::runtime::connection_registry::ConnectionId;
use crate::runtime::shutdown_reason::ConnectionShutdownReason;
use parking_lot::RwLock;
use paro_context::StatementCancelReason;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegistryKey {
    pub backend_pid: i32,
    pub cancel_secret: i32,
}

impl RegistryKey {
    pub fn new(backend_pid: i32, cancel_secret: i32) -> Self {
        Self {
            backend_pid,
            cancel_secret,
        }
    }
}

pub trait SessionExecutionHandle: Send + Sync {
    fn cancel_active_statement(&self, reason: StatementCancelReason) -> bool;
    fn request_connection_shutdown(&self, reason: ConnectionShutdownReason);
}

#[derive(Clone)]
struct SessionRegistryEntry {
    connection_id: ConnectionId,
    handle: Arc<dyn SessionExecutionHandle>,
}

#[derive(Default)]
struct SessionRegistryState {
    by_key: HashMap<RegistryKey, SessionRegistryEntry>,
    by_connection: HashMap<ConnectionId, RegistryKey>,
}

/// Tracks cancellable live sessions by both backend cancel key and connection id.
pub struct SessionExecutionRegistry {
    state: RwLock<SessionRegistryState>,
}

impl Default for SessionExecutionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionExecutionRegistry {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(SessionRegistryState::default()),
        }
    }

    pub fn register(
        &self,
        connection_id: ConnectionId,
        key: RegistryKey,
        handle: Arc<dyn SessionExecutionHandle>,
    ) {
        let mut state = self.state.write();

        if let Some(previous_key) = state.by_connection.insert(connection_id, key) {
            state.by_key.remove(&previous_key);
        }

        if let Some(stale_connection_id) = state.by_key.get(&key).map(|entry| entry.connection_id) {
            state.by_connection.remove(&stale_connection_id);
        }

        state.by_key.insert(
            key,
            SessionRegistryEntry {
                connection_id,
                handle,
            },
        );
    }

    pub fn unregister(&self, connection_id: ConnectionId) -> bool {
        let mut state = self.state.write();
        let Some(key) = state.by_connection.remove(&connection_id) else {
            return false;
        };
        state.by_key.remove(&key);
        true
    }

    pub fn cancel_by_key(&self, key: RegistryKey, reason: StatementCancelReason) -> bool {
        let handle = {
            let state = self.state.read();
            state
                .by_key
                .get(&key)
                .map(|entry| Arc::clone(&entry.handle))
        };
        handle
            .map(|handle| handle.cancel_active_statement(reason))
            .unwrap_or(false)
    }

    pub fn cancel_by_connection_id(
        &self,
        connection_id: ConnectionId,
        reason: StatementCancelReason,
    ) -> bool {
        let handle = {
            let state = self.state.read();
            state
                .by_connection
                .get(&connection_id)
                .and_then(|key| state.by_key.get(key))
                .map(|entry| Arc::clone(&entry.handle))
        };
        handle
            .map(|handle| handle.cancel_active_statement(reason))
            .unwrap_or(false)
    }

    pub fn shutdown_by_connection_id(
        &self,
        connection_id: ConnectionId,
        reason: ConnectionShutdownReason,
    ) -> bool {
        let handle = {
            let state = self.state.read();
            state
                .by_connection
                .get(&connection_id)
                .and_then(|key| state.by_key.get(key))
                .map(|entry| Arc::clone(&entry.handle))
        };
        if let Some(handle) = handle {
            handle.request_connection_shutdown(reason);
            true
        } else {
            false
        }
    }
}

impl std::fmt::Debug for SessionExecutionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read();
        f.debug_struct("SessionExecutionRegistry")
            .field("by_key_len", &state.by_key.len())
            .field("by_connection_len", &state.by_connection.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    struct TestHandle {
        cancelled: Mutex<usize>,
        shutdown: Mutex<usize>,
        cancel_reasons: Mutex<Vec<StatementCancelReason>>,
        shutdown_reasons: Mutex<Vec<ConnectionShutdownReason>>,
        cancel_result: bool,
    }

    impl TestHandle {
        fn with_cancel_result(cancel_result: bool) -> Self {
            Self {
                cancelled: Mutex::new(0),
                shutdown: Mutex::new(0),
                cancel_reasons: Mutex::new(Vec::new()),
                shutdown_reasons: Mutex::new(Vec::new()),
                cancel_result,
            }
        }
    }

    impl SessionExecutionHandle for TestHandle {
        fn cancel_active_statement(&self, reason: StatementCancelReason) -> bool {
            *self.cancelled.lock() += 1;
            self.cancel_reasons.lock().push(reason);
            self.cancel_result
        }

        fn request_connection_shutdown(&self, reason: ConnectionShutdownReason) {
            *self.shutdown.lock() += 1;
            self.shutdown_reasons.lock().push(reason);
        }
    }

    #[test]
    fn cancel_by_key_clones_handle_outside_lock() {
        let registry = SessionExecutionRegistry::new();
        let handle = Arc::new(TestHandle::with_cancel_result(true));
        let key = RegistryKey::new(11, 22);

        registry.register(7, key, handle.clone());

        assert!(registry.cancel_by_key(key, StatementCancelReason::UserRequest));
        assert_eq!(*handle.cancelled.lock(), 1);
        assert_eq!(
            handle.cancel_reasons.lock().as_slice(),
            &[StatementCancelReason::UserRequest]
        );
    }

    #[test]
    fn register_replaces_stale_entries_for_connection_and_key() {
        let registry = SessionExecutionRegistry::new();
        let first = Arc::new(TestHandle::with_cancel_result(true));
        let second = Arc::new(TestHandle::with_cancel_result(true));

        registry.register(1, RegistryKey::new(10, 10), first.clone());
        registry.register(2, RegistryKey::new(20, 20), second.clone());
        registry.register(1, RegistryKey::new(30, 30), second.clone());
        registry.register(3, RegistryKey::new(30, 30), first.clone());

        assert!(!registry.cancel_by_connection_id(1, StatementCancelReason::UserRequest));
        assert!(registry.cancel_by_connection_id(3, StatementCancelReason::UserRequest));
        assert_eq!(*first.cancelled.lock(), 1);
    }

    #[test]
    fn unregister_and_shutdown_use_connection_index() {
        let registry = SessionExecutionRegistry::new();
        let handle = Arc::new(TestHandle::with_cancel_result(false));
        let key = RegistryKey::new(42, 99);

        registry.register(9, key, handle.clone());
        assert!(registry.shutdown_by_connection_id(9, ConnectionShutdownReason::AdminShutdown));
        assert_eq!(*handle.shutdown.lock(), 1);
        assert_eq!(
            handle.shutdown_reasons.lock().as_slice(),
            &[ConnectionShutdownReason::AdminShutdown]
        );

        assert!(registry.unregister(9));
        assert!(!registry.cancel_by_key(key, StatementCancelReason::UserRequest));
    }
}
