// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opaque bound-relation boundary used while adapting Memo transformations.

use crate::plan::{CardinalityEstimate, UniqueKey};
use paro_common::types::LogicalType;
use std::sync::Arc;

use super::ColumnBinding;

/// A fact-backed relation reference whose implementation remains owned by an
/// external relational optimizer. It is legal only inside a transformation
/// transaction and must be consumed before physical planning.
#[derive(Debug)]
pub struct BoundReference {
    /// Stable identity of this reference occurrence. Unlike `PlanNodeId`, this
    /// survives optimizer passes that rebuild an operator shell.
    pub reference_id: u32,
    pub bindings: Vec<ColumnBinding>,
    pub types: Vec<LogicalType>,
    /// Immutable evidence resolved by the owning Memo, never by choosing or
    /// reconstructing a representative input tree.
    pub facts: Arc<BoundRelationFacts>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSourceColumn {
    pub source: usize,
    pub occurrence: usize,
    pub column: usize,
    pub rows: Option<CardinalityEstimate>,
    pub distinct: Option<u64>,
    pub unique: bool,
}

/// A source lineage is present only when every alternative supplies the same
/// complete source-column coverage. Unknown and partially covered paths must
/// not be confused with a covered path containing zero rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRelationFacts {
    pub unique_keys: Vec<UniqueKey>,
    /// Keys that remain unique when SQL grouping treats NULL values as equal.
    /// This is deliberately separate from ordinary/catalog uniqueness: a
    /// nullable UNIQUE constraint can admit several NULL tuples.
    pub grouping_unique_keys: Vec<UniqueKey>,
    pub source_lineage: Vec<Option<Vec<BoundSourceColumn>>>,
    pub contains_control_region: bool,
}

impl Default for BoundRelationFacts {
    fn default() -> Self {
        Self {
            unique_keys: Vec::new(),
            grouping_unique_keys: Vec::new(),
            source_lineage: Vec::new(),
            contains_control_region: true,
        }
    }
}

impl BoundReference {
    pub fn new(reference_id: u32, bindings: Vec<ColumnBinding>, types: Vec<LogicalType>) -> Self {
        assert_eq!(bindings.len(), types.len());
        Self {
            reference_id,
            bindings,
            types,
            facts: Arc::new(BoundRelationFacts::default()),
        }
    }

    pub fn with_facts(mut self, facts: Arc<BoundRelationFacts>) -> Self {
        assert_eq!(self.bindings.len(), facts.source_lineage.len());
        self.facts = facts;
        self
    }
}
