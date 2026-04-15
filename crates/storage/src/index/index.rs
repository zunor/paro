// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Index Base Trait
//!
//! Base interface for all index types.
//!
//! ## Design Notes
//!
//! The `Index` trait is the base interface for all indexes in Paro.
//! It provides common functionality shared by both bound and unbound indexes.
//!
//! - Bound indexes are attached to a table and can be used for queries
//! - Unbound indexes are used during WAL recovery before table binding

use paro_common::error::Result;

use super::IndexConstraintType;

/// Column index type (physical column ID in the table).
pub type ColumnId = u32;

/// Base trait for all index types.
///
/// This trait defines the common interface shared by all indexes,
/// whether they are bound to a table or not.
pub trait Index: Send + Sync {
    /// Returns the physical column IDs indexed by this index.
    ///
    /// These are the physical column indices in the table, not logical column indices.
    /// For example, given a table with columns (a INT, gen AS (2*a), b INT, c VARCHAR),
    /// an index on columns (a, c) would have column_ids [0, 2].
    fn column_ids(&self) -> &[ColumnId];

    /// Returns true if this is a bound index (attached to a table).
    fn is_bound(&self) -> bool;

    /// Returns the index type name (e.g., "ART", "HNSW").
    fn index_type(&self) -> &str;

    /// Returns the name of this index.
    fn index_name(&self) -> &str;

    /// Returns the constraint type of this index.
    fn constraint_type(&self) -> IndexConstraintType;

    /// Returns true if this index enforces uniqueness.
    #[inline]
    fn is_unique(&self) -> bool {
        self.constraint_type().is_unique()
    }

    /// Returns true if this index is a primary key index.
    #[inline]
    fn is_primary(&self) -> bool {
        self.constraint_type().is_primary()
    }

    /// Returns true if this index is a foreign key index.
    #[inline]
    fn is_foreign(&self) -> bool {
        self.constraint_type().is_foreign()
    }

    /// Commits the drop of this index, releasing all resources.
    ///
    /// This is called when the index is being dropped from the catalog.
    fn commit_drop(&mut self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock index for testing
    struct MockIndex {
        column_ids: Vec<ColumnId>,
        name: String,
        index_type: String,
        constraint_type: IndexConstraintType,
    }

    impl Index for MockIndex {
        fn column_ids(&self) -> &[ColumnId] {
            &self.column_ids
        }

        fn is_bound(&self) -> bool {
            false
        }

        fn index_type(&self) -> &str {
            &self.index_type
        }

        fn index_name(&self) -> &str {
            &self.name
        }

        fn constraint_type(&self) -> IndexConstraintType {
            self.constraint_type
        }

        fn commit_drop(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_index_properties() {
        let index = MockIndex {
            column_ids: vec![0, 2],
            name: "idx_test".to_string(),
            index_type: "ART".to_string(),
            constraint_type: IndexConstraintType::Unique,
        };

        assert_eq!(index.column_ids(), &[0, 2]);
        assert_eq!(index.index_name(), "idx_test");
        assert_eq!(index.index_type(), "ART");
        assert!(index.is_unique());
        assert!(!index.is_primary());
        assert!(!index.is_foreign());
    }

    #[test]
    fn test_primary_key_index() {
        let index = MockIndex {
            column_ids: vec![0],
            name: "pk_test".to_string(),
            index_type: "ART".to_string(),
            constraint_type: IndexConstraintType::Primary,
        };

        assert!(index.is_unique()); // Primary keys are also unique
        assert!(index.is_primary());
        assert!(!index.is_foreign());
    }
}
