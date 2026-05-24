// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Output schema carried by a physical plan node.

use paro_common::types::LogicalType;

#[derive(Debug, Clone, PartialEq)]
pub struct RowType {
    pub names: Box<[String]>,
    pub types: Box<[LogicalType]>,
}

impl RowType {
    pub fn new(names: Vec<String>, types: Vec<LogicalType>) -> Self {
        debug_assert_eq!(
            names.len(),
            types.len(),
            "physical row type names/types must stay aligned"
        );
        Self {
            names: names.into_boxed_slice(),
            types: types.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.types.len()
    }
}
