// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! State tracked while replaying WAL entries.

use crate::wal::wal_entry::WalEntry;

/// State maintained during WAL replay.
///
/// This tracks:
/// - Current table context for data operations
/// - Checkpoint information for recovery decisions
/// - WAL version for format compatibility
#[derive(Debug)]
pub struct ReplayState {
    /// Current schema name for data operations
    pub current_schema: Option<String>,
    /// Current table name for data operations
    pub current_table: Option<String>,
    /// Checkpoint marker (if checkpoint entry found)
    pub checkpoint_marker: Option<u64>,
    /// Position of the checkpoint entry in the WAL
    pub checkpoint_position: Option<u64>,
    /// WAL version
    pub wal_version: u64,
    /// Current position in the WAL (for error reporting)
    pub current_position: u64,
    /// Number of entries replayed
    pub entries_replayed: u64,
    /// Whether we're in deserialize-only mode (first pass)
    pub deserialize_only: bool,
}

impl Default for ReplayState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayState {
    /// Create a new replay state.
    pub fn new() -> Self {
        Self {
            current_schema: None,
            current_table: None,
            checkpoint_marker: None,
            checkpoint_position: None,
            wal_version: 2,
            current_position: 0,
            entries_replayed: 0,
            deserialize_only: false,
        }
    }

    /// Create a replay state for deserialize-only mode (first pass).
    pub fn deserialize_only() -> Self {
        Self {
            deserialize_only: true,
            ..Self::new()
        }
    }

    /// Set the current table context from a USE_TABLE entry.
    pub fn set_current_table(&mut self, schema: String, table: String) {
        self.current_schema = Some(schema);
        self.current_table = Some(table);
    }

    /// Clear the current table context.
    pub fn clear_current_table(&mut self) {
        self.current_schema = None;
        self.current_table = None;
    }

    /// Get the current table context.
    pub fn get_current_table(&self) -> Option<(&str, &str)> {
        match (&self.current_schema, &self.current_table) {
            (Some(schema), Some(table)) => Some((schema.as_str(), table.as_str())),
            _ => None,
        }
    }

    /// Record a checkpoint entry.
    pub fn set_checkpoint(&mut self, checkpoint_marker: u64, position: u64) {
        self.checkpoint_marker = Some(checkpoint_marker);
        self.checkpoint_position = Some(position);
    }

    /// Check if a checkpoint was found.
    pub fn has_checkpoint(&self) -> bool {
        self.checkpoint_marker.is_some()
    }

    /// Update state based on a WAL entry.
    ///
    /// This handles state transitions like USE_TABLE and CHECKPOINT.
    pub fn process_entry(&mut self, entry: &WalEntry, position: u64) {
        self.current_position = position;
        self.entries_replayed += 1;

        match entry {
            WalEntry::UseTable {
                schema_name,
                table_name,
            } => {
                self.set_current_table(schema_name.clone(), table_name.clone());
            }
            WalEntry::Checkpoint { checkpoint_marker } => {
                self.set_checkpoint(*checkpoint_marker, position);
            }
            _ => {}
        }
    }

    /// Reset state for a new replay pass.
    pub fn reset(&mut self) {
        self.current_schema = None;
        self.current_table = None;
        self.current_position = 0;
        self.entries_replayed = 0;
        // Keep checkpoint info and wal_version
    }
}

/// Result of WAL replay.
#[derive(Debug)]
pub struct ReplayResult {
    /// Number of entries successfully replayed
    pub entries_replayed: u64,
    /// Last successful offset in the WAL
    pub last_successful_offset: u64,
    /// Whether all entries were replayed successfully
    pub all_succeeded: bool,
    /// Checkpoint information if found
    pub checkpoint_info: Option<CheckpointInfo>,
    /// Error message if replay failed
    pub error: Option<String>,
    /// Whether the checkpoint was verified against the database header
    pub checkpoint_verified: bool,
}

/// Checkpoint information from WAL.
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// Logical checkpoint marker persisted in metadata store and WAL.
    pub checkpoint_marker: u64,
    /// Position of the checkpoint entry in the WAL
    pub wal_position: u64,
}

impl ReplayResult {
    /// Create a successful replay result.
    pub fn success(entries_replayed: u64, last_offset: u64) -> Self {
        Self {
            entries_replayed,
            last_successful_offset: last_offset,
            all_succeeded: true,
            checkpoint_info: None,
            error: None,
            checkpoint_verified: false,
        }
    }

    /// Create a partial replay result (some entries failed).
    pub fn partial(entries_replayed: u64, last_offset: u64, error: String) -> Self {
        Self {
            entries_replayed,
            last_successful_offset: last_offset,
            all_succeeded: false,
            checkpoint_info: None,
            error: Some(error),
            checkpoint_verified: false,
        }
    }

    /// Set checkpoint information.
    pub fn with_checkpoint(mut self, info: CheckpointInfo) -> Self {
        self.checkpoint_info = Some(info);
        self
    }

    /// Set whether the checkpoint was verified against the database header.
    ///
    /// When true, it means the checkpoint marker in the WAL matches the
    /// metadata-store checkpoint marker, indicating the checkpoint completed
    /// successfully and no WAL replay was needed.
    pub fn with_checkpoint_verified(mut self, verified: bool) -> Self {
        self.checkpoint_verified = verified;
        self
    }

    /// Check if the checkpoint was verified and no replay was needed.
    pub fn checkpoint_was_clean(&self) -> bool {
        self.checkpoint_verified && self.entries_replayed == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_state_new() {
        let state = ReplayState::new();
        assert!(state.current_schema.is_none());
        assert!(state.current_table.is_none());
        assert!(!state.has_checkpoint());
        assert_eq!(state.entries_replayed, 0);
    }

    #[test]
    fn test_replay_state_set_table() {
        let mut state = ReplayState::new();
        state.set_current_table("main".to_string(), "users".to_string());

        let (schema, table) = state.get_current_table().unwrap();
        assert_eq!(schema, "main");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_replay_state_process_use_table() {
        let mut state = ReplayState::new();

        let entry = WalEntry::UseTable {
            schema_name: "test".to_string(),
            table_name: "items".to_string(),
        };

        state.process_entry(&entry, 100);

        let (schema, table) = state.get_current_table().unwrap();
        assert_eq!(schema, "test");
        assert_eq!(table, "items");
        assert_eq!(state.entries_replayed, 1);
    }

    #[test]
    fn test_replay_state_process_checkpoint() {
        let mut state = ReplayState::new();

        let entry = WalEntry::Checkpoint {
            checkpoint_marker: 42,
        };

        state.process_entry(&entry, 500);

        assert!(state.has_checkpoint());
        assert_eq!(state.checkpoint_marker, Some(42));
        assert_eq!(state.checkpoint_position, Some(500));
    }

    #[test]
    fn test_replay_state_reset() {
        let mut state = ReplayState::new();
        state.set_current_table("main".to_string(), "users".to_string());
        state.entries_replayed = 10;
        state.set_checkpoint(42, 500);

        state.reset();

        assert!(state.current_schema.is_none());
        assert!(state.current_table.is_none());
        assert_eq!(state.entries_replayed, 0);
        // Checkpoint info should be preserved
        assert!(state.has_checkpoint());
    }

    #[test]
    fn test_replay_result_success() {
        let result = ReplayResult::success(100, 5000);
        assert!(result.all_succeeded);
        assert_eq!(result.entries_replayed, 100);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_replay_result_partial() {
        let result = ReplayResult::partial(50, 2500, "Checksum error".to_string());
        assert!(!result.all_succeeded);
        assert_eq!(result.entries_replayed, 50);
        assert!(result.error.is_some());
    }
}
