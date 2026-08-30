// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Bound Index
//!
//! Interface for indexes bound to a table.
//!
//! ## Design Notes
//!
//! A `BoundIndex` is an index that has been bound to a specific table.
//! It extends the base `Index` trait with operations for:
//!
//! - Appending data (INSERT)
//! - Deleting data (DELETE)
//! - Inserting with constraint checking
//! - Merging indexes
//! - Vacuuming (space reclamation)
//! - Serialization to disk

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::{ExactRowSet, Index, IndexStorageInfo};
use crate::index::predicate::Predicate;
use crate::index::predicate_result::{intersect, PredicateResult};

#[cfg(test)]
use super::IndexConstraintType;
#[cfg(test)]
use crate::index::ColumnId;

// =============================================================================
// Index Append Mode
// =============================================================================

/// Mode for appending data to an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexAppendMode {
    /// Default mode: fail on duplicates for unique indexes
    #[default]
    Default,
    /// Ignore duplicates (for INSERT OR IGNORE)
    IgnoreDuplicates,
    /// Insert duplicates anyway (for special cases)
    InsertDuplicates,
}

/// Information for index append operations.
#[derive(Debug, Clone, Default)]
pub struct IndexAppendInfo {
    /// Append mode
    pub append_mode: IndexAppendMode,
    /// Indexes to delete from on conflict (for UPSERT)
    pub delete_index_names: Vec<String>,
}

/// Candidate and proof sets produced by one index predicate evaluation.
///
/// Construction intersects the proof with the candidate set, making
/// `guaranteed ⊆ candidates` structural rather than a convention shared by
/// independent evaluator implementations.
#[derive(Debug, Clone)]
pub struct IndexPredicateEvaluation {
    pub candidates: PredicateResult,
    /// `None` encodes the strongest proof: the candidate set itself is exact.
    /// Keeping that state structural avoids cloning a large bitmap merely to
    /// store the same set twice.
    guaranteed: Option<PredicateResult>,
}

/// Proof that one immutable scalar access path was built from every row in a
/// segment-local row-id domain. Construction is fallible so callers cannot
/// turn an observed prefix into a completeness claim with a boolean flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentLocalComplete {
    row_count: u64,
}

impl SegmentLocalComplete {
    pub(crate) fn prove(indexed_rows: u64, segment_rows: u64) -> Result<Self> {
        if indexed_rows != segment_rows {
            return Err(paro_error::data_corrupted(format!(
                "scalar index row coverage mismatch: indexed={indexed_rows}, segment={segment_rows}"
            )));
        }
        Ok(Self {
            row_count: indexed_rows,
        })
    }

    pub(crate) const fn covers(self, segment_rows: u64) -> bool {
        self.row_count == segment_rows
    }

    pub(crate) const fn row_count(self) -> u64 {
        self.row_count
    }
}

/// One predicate access path together with the scope in which its row IDs are
/// valid. Exactness is granted by the segment build/loader boundary, never by
/// an index implementation reporting a global boolean capability.
#[derive(Clone)]
pub(crate) struct PredicateIndexBinding {
    index: Arc<dyn BoundIndex>,
    complete_scalar: Option<SegmentLocalComplete>,
}

impl PredicateIndexBinding {
    pub(crate) fn candidate(index: Arc<dyn BoundIndex>) -> Self {
        Self {
            index,
            complete_scalar: None,
        }
    }

    pub(crate) fn complete_scalar(
        index: Arc<dyn BoundIndex>,
        completeness: SegmentLocalComplete,
    ) -> Self {
        Self {
            index,
            complete_scalar: Some(completeness),
        }
    }

    pub(crate) fn index(&self) -> &Arc<dyn BoundIndex> {
        &self.index
    }

    pub(crate) const fn is_complete_for(&self, segment_rows: u64) -> bool {
        match self.complete_scalar {
            Some(completeness) => completeness.covers(segment_rows),
            None => false,
        }
    }
}

impl IndexPredicateEvaluation {
    pub fn new(candidates: PredicateResult, guaranteed: PredicateResult) -> Self {
        // `Unknown` means universal/no information for a candidate set, but
        // no information is the empty set for a proof. Normalize before using
        // the generic candidate intersection algebra.
        let guaranteed = if matches!(guaranteed, PredicateResult::Unknown) {
            PredicateResult::NoneMatch
        } else {
            intersect(&candidates, &guaranteed)
        };
        Self {
            candidates,
            guaranteed: Some(guaranteed),
        }
    }

    /// Construct an exact predicate answer. Exactness is proof metadata, not
    /// something consumers should rediscover by comparing two potentially
    /// large bitmaps.
    pub fn exact(candidates: PredicateResult) -> Self {
        if matches!(candidates, PredicateResult::Unknown) {
            return Self::candidates_only(candidates);
        }
        Self {
            candidates,
            guaranteed: None,
        }
    }

    pub const fn is_exact(&self) -> bool {
        self.guaranteed.is_none()
    }

    pub fn guaranteed(&self) -> &PredicateResult {
        self.guaranteed.as_ref().unwrap_or(&self.candidates)
    }

    pub fn into_parts(self) -> (PredicateResult, PredicateResult) {
        match self.guaranteed {
            Some(guaranteed) => (self.candidates, guaranteed),
            None => {
                let guaranteed = self.candidates.clone();
                (self.candidates, guaranteed)
            }
        }
    }

    pub fn candidates_only(candidates: PredicateResult) -> Self {
        Self::new(candidates, PredicateResult::NoneMatch)
    }
}

impl IndexAppendInfo {
    /// Creates a new IndexAppendInfo with default mode.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an IndexAppendInfo with the specified mode.
    pub fn with_mode(mode: IndexAppendMode) -> Self {
        Self {
            append_mode: mode,
            delete_index_names: Vec::new(),
        }
    }
}

// =============================================================================
// Delta Index Type
// =============================================================================

/// Type of delta index.
///
/// Delta indexes are temporary indexes used during transactions
/// to track local changes before commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaIndexType {
    /// Not a delta index
    #[default]
    None,
    /// Local append delta (uncommitted inserts)
    LocalAppend,
    /// Local delete delta (uncommitted deletes)
    LocalDelete,
    /// Added during checkpoint
    AddedDuringCheckpoint,
    /// Removed during checkpoint
    RemovedDuringCheckpoint,
    /// Deleted rows in use
    DeletedRowsInUse,
}

// =============================================================================
// Bound Index Trait
// =============================================================================

/// A bound index attached to a table.
///
/// This trait extends `Index` with operations for data manipulation
/// and maintenance. Implementations must be thread-safe.
pub trait BoundIndex: Index {
    /// Returns the physical types of the indexed columns.
    fn physical_types(&self) -> &[paro_common::types::LogicalType];

    /// Returns the logical types of the indexed columns.
    fn logical_types(&self) -> &[LogicalType];

    /// Returns the delta index type.
    fn delta_index_type(&self) -> DeltaIndexType {
        DeltaIndexType::None
    }

    // =========================================================================
    // Predicate Evaluation
    // =========================================================================

    /// Evaluate a predicate using this index.
    ///
    /// Returns PredicateResult::Unknown if the index cannot evaluate the predicate.
    fn evaluate_predicate(&self, _predicate: &Predicate) -> PredicateResult {
        PredicateResult::Unknown
    }

    /// Evaluate candidate and guaranteed-true rows together. Indexes without
    /// proof semantics inherit a candidate-only result.
    fn evaluate_predicate_with_proof(&self, predicate: &Predicate) -> IndexPredicateEvaluation {
        IndexPredicateEvaluation::candidates_only(self.evaluate_predicate(predicate))
    }

    /// Compile an exact index-native membership representation. The evaluator
    /// only accepts this result from a complete segment-local binding.
    fn compile_exact_row_set(&self, _predicate: &Predicate) -> Option<Arc<dyn ExactRowSet>> {
        None
    }

    /// Whether this index can establish guaranteed-true rows in addition to
    /// candidate pruning. Evaluators use this capability to avoid probing
    /// candidate-only indexes after a higher-priority candidate result wins.
    fn provides_predicate_proof(&self) -> bool {
        false
    }

    // =========================================================================
    // Data Manipulation
    // =========================================================================

    /// Appends data to the index.
    ///
    /// # Arguments
    /// * `chunk` - Data chunk containing the indexed column values
    /// * `row_ids` - Vector of row IDs corresponding to each row in the chunk
    ///
    /// # Returns
    /// Ok(()) on success, or an error if the append fails
    fn append(&self, chunk: &Chunk, row_ids: &Vector) -> Result<()>;

    /// Appends data with constraint checking.
    ///
    /// # Arguments
    /// * `chunk` - Data chunk containing the indexed column values
    /// * `row_ids` - Vector of row IDs
    /// * `info` - Append information including mode and conflict handling
    fn append_with_info(
        &self,
        chunk: &Chunk,
        row_ids: &Vector,
        info: &IndexAppendInfo,
    ) -> Result<()> {
        // Default implementation ignores info and calls append
        let _ = info;
        self.append(chunk, row_ids)
    }

    /// Verifies that data can be appended without constraint violations.
    ///
    /// This is used for constraint checking before the actual append.
    fn verify_append(&self, chunk: &Chunk, info: &IndexAppendInfo) -> Result<()> {
        // Default: no verification needed for non-unique indexes
        let _ = (chunk, info);
        Ok(())
    }

    /// Deletes entries from the index.
    ///
    /// # Arguments
    /// * `entries` - Data chunk containing the values to delete
    /// * `row_ids` - Vector of row IDs to delete
    ///
    /// # Returns
    /// The number of entries successfully deleted
    fn delete(&self, entries: &Chunk, row_ids: &Vector) -> Result<usize>;

    /// Inserts data with constraint checking.
    ///
    /// Unlike append, insert performs full constraint verification
    /// and may fail if constraints are violated.
    fn insert(&self, chunk: &Chunk, row_ids: &Vector) -> Result<()>;

    /// Inserts data with additional info.
    fn insert_with_info(
        &self,
        chunk: &Chunk,
        row_ids: &Vector,
        info: &IndexAppendInfo,
    ) -> Result<()> {
        let _ = info;
        self.insert(chunk, row_ids)
    }

    // =========================================================================
    // Index Maintenance
    // =========================================================================

    /// Merges another index into this one.
    ///
    /// Used for combining delta indexes with the main index during commit.
    ///
    /// # Returns
    /// true if the merge was successful, false otherwise
    fn merge_indexes(&self, other: &dyn BoundIndex) -> Result<bool>;

    /// Vacuums the index, reclaiming space from deleted entries.
    fn vacuum(&self);

    // =========================================================================
    // Delta Index Support
    // =========================================================================

    /// Returns true if this index supports delta indexes.
    fn supports_delta_indexes(&self) -> bool {
        false
    }

    /// Creates a delta index for tracking local changes.
    ///
    /// Only called if `supports_delta_indexes()` returns true.
    fn create_delta_index(&self, delta_type: DeltaIndexType) -> Result<Arc<dyn BoundIndex>> {
        let _ = delta_type;
        Err(paro_common::error::not_implemented(
            "Delta indexes not supported",
        ))
    }

    // =========================================================================
    // Memory and Serialization
    // =========================================================================

    /// Returns the in-memory size of the index in bytes.
    fn get_in_memory_size(&self) -> usize;

    /// Serializes the index to disk.
    ///
    /// # Returns
    /// IndexStorageInfo containing the serialization metadata
    fn serialize_to_disk(&self) -> Result<IndexStorageInfo>;

    /// Serializes the index to WAL.
    ///
    /// This may be different from disk serialization for incremental updates.
    fn serialize_to_wal(&self) -> Result<IndexStorageInfo> {
        // Default: same as disk serialization
        self.serialize_to_disk()
    }

    // =========================================================================
    // Verification
    // =========================================================================

    /// Verifies the integrity of the index.
    fn verify(&self) -> Result<()> {
        Ok(())
    }

    /// Returns a string representation of the index for debugging.
    fn to_string_debug(&self, display_ascii: bool) -> String {
        let _ = display_ascii;
        format!("{}({})", self.index_type(), self.index_name())
    }

    /// Verifies that allocations are consistent.
    fn verify_allocations(&self) -> Result<()> {
        Ok(())
    }
}

/// Helper struct for bound-index tests.
#[cfg(test)]
pub struct BoundIndexBase {
    /// Index name
    pub name: String,
    /// Index type name
    pub index_type: String,
    /// Constraint type
    pub constraint_type: IndexConstraintType,
    /// Physical column IDs
    pub column_ids: Vec<ColumnId>,
    /// Logical types
    pub logical_types: Vec<LogicalType>,
    /// Delta index type
    pub delta_index_type: DeltaIndexType,
}

#[cfg(test)]
impl BoundIndexBase {
    /// Creates a new BoundIndexBase.
    pub fn new(
        name: impl Into<String>,
        index_type: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_ids: Vec<ColumnId>,
        logical_types: Vec<LogicalType>,
    ) -> Self {
        Self {
            name: name.into(),
            index_type: index_type.into(),
            constraint_type,
            column_ids,
            logical_types,
            delta_index_type: DeltaIndexType::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_append_defaults() {
        assert_eq!(DeltaIndexType::default(), DeltaIndexType::None);
        assert_eq!(IndexAppendMode::default(), IndexAppendMode::Default);

        let info = IndexAppendInfo::new();
        assert_eq!(info.append_mode, IndexAppendMode::Default);
        assert!(info.delete_index_names.is_empty());

        let info = IndexAppendInfo::with_mode(IndexAppendMode::IgnoreDuplicates);
        assert_eq!(info.append_mode, IndexAppendMode::IgnoreDuplicates);
    }

    #[test]
    fn test_bound_index_base() {
        let base = BoundIndexBase::new(
            "test_idx",
            "ART",
            IndexConstraintType::Unique,
            vec![0, 1],
            vec![LogicalType::Integer, LogicalType::Varchar],
        );

        assert_eq!(base.name, "test_idx");
        assert_eq!(base.index_type, "ART");
        assert_eq!(base.constraint_type, IndexConstraintType::Unique);
        assert_eq!(base.column_ids, vec![0, 1]);
        assert_eq!(base.delta_index_type, DeltaIndexType::None);
        assert_eq!(base.logical_types.len(), 2);
    }

    #[test]
    fn unknown_proof_is_normalized_to_no_proof() {
        let evaluation =
            IndexPredicateEvaluation::new(PredicateResult::AllMatch, PredicateResult::Unknown);
        assert!(matches!(
            evaluation.guaranteed(),
            PredicateResult::NoneMatch
        ));

        let unknown = IndexPredicateEvaluation::exact(PredicateResult::Unknown);
        assert!(!unknown.is_exact());
        assert!(matches!(unknown.guaranteed(), PredicateResult::NoneMatch));
    }
}
