// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tracks active client connections to the database instance.

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// Unique identifier for a connection.
pub type ConnectionId = u64;

/// Trait for objects that can be tracked by the connection registry.
pub trait ConnectionHandle: Send + Sync {
    /// Get the unique connection ID.
    fn connection_id(&self) -> ConnectionId;

    /// Check if the connection is still active.
    fn is_active(&self) -> bool;

    /// Human-readable connection description.
    fn description(&self) -> String;
}

/// Tracks active client connections to the database instance.
///
/// It tracks all active connections and provides methods to:
/// - Add/remove connections
/// - List all connections
/// - Assign unique connection IDs
pub struct ConnectionRegistry {
    /// Map of connection ID to weak reference of the connection.
    /// Using weak references allows connections to be dropped when
    /// no longer in use, while still being tracked here.
    connections: RwLock<HashMap<ConnectionId, Weak<dyn ConnectionHandle>>>,

    /// Lock for connection modifications.
    connections_lock: Mutex<()>,

    /// Current connection count (may include stale entries).
    connection_count: AtomicU64,

    /// Next connection ID to assign.
    next_connection_id: AtomicU64,
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionRegistry {
    /// Create a new connection registry.
    pub fn new() -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            connections_lock: Mutex::new(()),
            connection_count: AtomicU64::new(0),
            next_connection_id: AtomicU64::new(1),
        }
    }

    /// Assign a unique connection ID.
    pub fn assign_connection_id(&self) -> ConnectionId {
        self.next_connection_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the next connection ID that will be assigned.
    pub fn current_connection_id(&self) -> ConnectionId {
        self.next_connection_id.load(Ordering::SeqCst)
    }

    /// Add a connection to the manager.
    pub fn add_connection(&self, connection: Arc<dyn ConnectionHandle>) {
        let _guard = self.connections_lock.lock();
        let id = connection.connection_id();
        let mut connections = self.connections.write();
        connections.insert(id, Arc::downgrade(&connection));
        self.connection_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Remove a connection from the manager.
    pub fn remove_connection(&self, connection_id: ConnectionId) -> bool {
        let _guard = self.connections_lock.lock();
        let mut connections = self.connections.write();
        if connections.remove(&connection_id).is_some() {
            self.connection_count.fetch_sub(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// Get a list of all active connections.
    ///
    /// This cleans up stale (dropped) connections and returns
    /// only the ones that are still alive.
    pub fn get_connection_list(&self) -> Vec<Arc<dyn ConnectionHandle>> {
        let connections = self.connections.read();
        let mut result = Vec::new();
        let mut stale_ids = Vec::new();

        for (id, weak) in connections.iter() {
            if let Some(conn) = weak.upgrade() {
                if conn.is_active() {
                    result.push(conn);
                } else {
                    stale_ids.push(*id);
                }
            } else {
                stale_ids.push(*id);
            }
        }

        // Clean up stale connections if any
        if !stale_ids.is_empty() {
            drop(connections);
            let _guard = self.connections_lock.lock();
            let mut connections = self.connections.write();
            for id in stale_ids {
                connections.remove(&id);
                self.connection_count.fetch_sub(1, Ordering::SeqCst);
            }
        }

        result
    }

    /// Get the approximate connection count.
    ///
    /// This may include stale connections that haven't been cleaned up yet.
    pub fn get_connection_count(&self) -> u64 {
        self.connection_count.load(Ordering::SeqCst)
    }

    /// Get the exact connection count by checking all connections.
    pub fn get_active_connection_count(&self) -> usize {
        self.get_connection_list().len()
    }

    /// Check if there are any active connections.
    pub fn has_connections(&self) -> bool {
        self.get_connection_count() > 0
    }

    /// Get a specific connection by ID.
    pub fn get_connection(&self, connection_id: ConnectionId) -> Option<Arc<dyn ConnectionHandle>> {
        let connections = self.connections.read();
        connections
            .get(&connection_id)
            .and_then(|weak| weak.upgrade())
            .filter(|conn| conn.is_active())
    }
}

impl std::fmt::Debug for ConnectionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionRegistry")
            .field("connection_count", &self.get_connection_count())
            .field("next_connection_id", &self.current_connection_id())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConnection {
        id: ConnectionId,
        active: std::sync::atomic::AtomicBool,
    }

    impl TestConnection {
        fn new(id: ConnectionId) -> Self {
            Self {
                id,
                active: std::sync::atomic::AtomicBool::new(true),
            }
        }

        fn deactivate(&self) {
            self.active.store(false, Ordering::SeqCst);
        }
    }

    impl ConnectionHandle for TestConnection {
        fn connection_id(&self) -> ConnectionId {
            self.id
        }

        fn is_active(&self) -> bool {
            self.active.load(Ordering::SeqCst)
        }

        fn description(&self) -> String {
            format!("TestConnection({})", self.id)
        }
    }

    #[test]
    fn test_connection_registry_basic() {
        let manager = ConnectionRegistry::new();

        // Assign IDs
        let id1 = manager.assign_connection_id();
        let id2 = manager.assign_connection_id();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        // Add connections
        let conn1 = Arc::new(TestConnection::new(id1));
        let conn2 = Arc::new(TestConnection::new(id2));

        manager.add_connection(conn1.clone());
        manager.add_connection(conn2.clone());

        assert_eq!(manager.get_connection_count(), 2);
        assert_eq!(manager.get_active_connection_count(), 2);
    }

    #[test]
    fn test_connection_registry_remove() {
        let manager = ConnectionRegistry::new();

        let id = manager.assign_connection_id();
        let conn = Arc::new(TestConnection::new(id));

        manager.add_connection(conn.clone());
        assert_eq!(manager.get_connection_count(), 1);

        assert!(manager.remove_connection(id));
        assert_eq!(manager.get_connection_count(), 0);

        // Remove non-existent
        assert!(!manager.remove_connection(999));
    }

    #[test]
    fn test_connection_registry_stale_cleanup() {
        let manager = ConnectionRegistry::new();

        let id1 = manager.assign_connection_id();
        let id2 = manager.assign_connection_id();

        let conn1 = Arc::new(TestConnection::new(id1));
        let conn2 = Arc::new(TestConnection::new(id2));

        manager.add_connection(conn1.clone());
        manager.add_connection(conn2.clone());

        // Deactivate conn1
        conn1.deactivate();

        // get_connection_list should clean up inactive connections
        let active = manager.get_connection_list();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].connection_id(), id2);
    }

    #[test]
    fn test_connection_registry_get_connection() {
        let manager = ConnectionRegistry::new();

        let id = manager.assign_connection_id();
        let conn = Arc::new(TestConnection::new(id));

        manager.add_connection(conn.clone());

        // Get existing connection
        let retrieved = manager.get_connection(id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().connection_id(), id);

        // Get non-existent connection
        assert!(manager.get_connection(999).is_none());
    }

    #[test]
    fn test_connection_registry_weak_reference() {
        let manager = ConnectionRegistry::new();

        let id = manager.assign_connection_id();
        {
            let conn = Arc::new(TestConnection::new(id));
            manager.add_connection(conn);
            // conn is dropped here
        }

        // Connection should be gone (weak reference expired)
        assert!(manager.get_connection(id).is_none());
    }
}
