// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opaque bound-relation boundary used while adapting Memo transformations.

use paro_common::types::LogicalType;

use super::ColumnBinding;

/// A schema-only relation reference whose implementation remains owned by an
/// external relational optimizer. It is legal only inside a transformation
/// transaction and must be consumed before physical planning.
#[derive(Debug)]
pub struct BoundReference {
    /// Stable identity of this reference occurrence. Unlike `PlanNodeId`, this
    /// survives optimizer passes that rebuild an operator shell.
    pub reference_id: u32,
    pub bindings: Vec<ColumnBinding>,
    pub types: Vec<LogicalType>,
}

impl BoundReference {
    pub fn new(reference_id: u32, bindings: Vec<ColumnBinding>, types: Vec<LogicalType>) -> Self {
        assert_eq!(bindings.len(), types.len());
        Self {
            reference_id,
            bindings,
            types,
        }
    }
}
