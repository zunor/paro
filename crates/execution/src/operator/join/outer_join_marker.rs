//! Outer join marker helpers for nested loop joins.

use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

/// Tracks whether build-side rows found at least one probe-side partner.
#[derive(Debug)]
pub struct OuterJoinMarker {
    enabled: bool,
    found_match: Mutex<Vec<bool>>,
}

/// Stateful scan cursor for unmatched/matched build-side rows.
#[derive(Debug, Default, Clone)]
pub struct OuterJoinScanState {
    pub chunk_idx: usize,
    pub row_idx: usize,
    pub global_row_idx: usize,
}

impl OuterJoinMarker {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            found_match: Mutex::new(Vec::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn add_rows(&self, count: usize) {
        if !self.enabled || count == 0 {
            return;
        }
        let mut found = self.found_match.lock().unwrap();
        let new_len = found.len() + count;
        found.resize(new_len, false);
    }

    pub fn reset(&self) {
        if !self.enabled {
            return;
        }
        let mut found = self.found_match.lock().unwrap();
        found.fill(false);
    }

    pub fn set_match(&self, position: usize) {
        if !self.enabled {
            return;
        }
        let mut found = self.found_match.lock().unwrap();
        if let Some(entry) = found.get_mut(position) {
            *entry = true;
        }
    }

    pub fn scan(
        &self,
        chunks: &[Chunk],
        state: &mut OuterJoinScanState,
        emit_found: bool,
        result: &mut Chunk,
    ) -> Result<usize> {
        if !self.enabled {
            result.set_cardinality(0);
            return Ok(0);
        }

        let found = self.found_match.lock().unwrap();
        let mut count = 0;

        while state.chunk_idx < chunks.len() && count < result.capacity() {
            let chunk = &chunks[state.chunk_idx];
            while state.row_idx < chunk.size() && count < result.capacity() {
                let matched = found.get(state.global_row_idx).copied().unwrap_or(false);
                if matched == emit_found {
                    for col_idx in 0..chunk.column_count() {
                        let source = chunk.column(col_idx).ok_or_else(|| {
                            paro_error::internal(format!(
                                "Build chunk column {} not found during outer scan",
                                col_idx
                            ))
                        })?;
                        let target = result.column_mut(col_idx).ok_or_else(|| {
                            paro_error::internal(format!(
                                "Result chunk column {} not found during outer scan",
                                col_idx
                            ))
                        })?;
                        let value = source.get_value(state.row_idx);
                        target.set_value(count, &value);
                    }
                    count += 1;
                }
                state.row_idx += 1;
                state.global_row_idx += 1;
            }

            if state.row_idx >= chunk.size() {
                state.chunk_idx += 1;
                state.row_idx = 0;
            }
        }

        result.set_cardinality(count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::{OuterJoinMarker, OuterJoinScanState};
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use std::sync::Arc;

    #[test]
    fn scan_returns_only_unmatched_rows() {
        let chunks = vec![Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(
            &[10, 20, 30],
        ))])];
        let marker = OuterJoinMarker::new(true);
        marker.add_rows(3);
        marker.set_match(0);
        marker.set_match(2);

        let mut state = OuterJoinScanState::default();
        let mut result = Chunk::initialize(&[LogicalType::Integer], 3);

        let count = marker
            .scan(&chunks, &mut state, false, &mut result)
            .expect("scan should succeed");

        assert_eq!(count, 1);
        assert_eq!(result.size(), 1);
        assert_eq!(result.data[0].get_value(0).to_string(), "20");
    }
}
