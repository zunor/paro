// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! State tracked while replaying WAL entries.

/// State maintained during WAL replay.
#[derive(Debug)]
pub struct ReplayState {
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
    pub fn new() -> Self {
        Self {
            wal_version: 2,
            current_position: 0,
            entries_replayed: 0,
            deserialize_only: false,
        }
    }

    pub fn deserialize_only() -> Self {
        Self {
            deserialize_only: true,
            ..Self::new()
        }
    }

    pub fn process_entry(&mut self, position: u64) {
        self.current_position = position;
        self.entries_replayed += 1;
    }

    pub fn reset(&mut self) {
        self.current_position = 0;
        self.entries_replayed = 0;
    }
}

/// Result of WAL replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayResult {
    /// Number of entries successfully replayed
    pub entries_replayed: u64,
    /// Last successful offset in the WAL
    pub last_successful_offset: u64,
    /// Whether all entries were replayed successfully
    pub all_succeeded: bool,
    /// Error message if replay failed
    pub error: Option<String>,
}

impl ReplayResult {
    pub fn success(entries_replayed: u64, last_offset: u64) -> Self {
        Self {
            entries_replayed,
            last_successful_offset: last_offset,
            all_succeeded: true,
            error: None,
        }
    }

    pub fn partial(entries_replayed: u64, last_offset: u64, error: String) -> Self {
        Self {
            entries_replayed,
            last_successful_offset: last_offset,
            all_succeeded: false,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_state_new() {
        let state = ReplayState::new();
        assert_eq!(state.entries_replayed, 0);
    }

    #[test]
    fn test_replay_state_process_entry() {
        let mut state = ReplayState::new();
        state.process_entry(100);
        assert_eq!(state.current_position, 100);
        assert_eq!(state.entries_replayed, 1);
    }

    #[test]
    fn test_replay_state_reset() {
        let mut state = ReplayState::new();
        state.current_position = 99;
        state.entries_replayed = 10;

        state.reset();

        assert_eq!(state.current_position, 0);
        assert_eq!(state.entries_replayed, 0);
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
