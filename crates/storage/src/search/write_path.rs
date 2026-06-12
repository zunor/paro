// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::tablet::ColumnId;

use super::inline_sink::SearchInlineBuilderSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextWriteBinding {
    pub(crate) column_id: ColumnId,
    pub(crate) config: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SparseWriteBinding {
    pub(crate) column_id: ColumnId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SearchWritePlan {
    pub(crate) fulltext: Vec<FullTextWriteBinding>,
    pub(crate) sparse: Vec<SparseWriteBinding>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SearchWriteContext {
    pub(crate) plan: SearchWritePlan,
    pub(crate) inline_builders: SearchInlineBuilderSet,
}

impl SearchWriteContext {
    pub(crate) fn is_empty(&self) -> bool {
        self.plan.is_empty() && self.inline_builders.is_empty()
    }

    pub(crate) fn matches_write_identity(&self, other: &Self) -> bool {
        if self.plan != other.plan {
            return false;
        }
        let left = self.inline_builders.entries();
        let right = other.inline_builders.entries();
        left.len() == right.len()
            && left.iter().zip(right).all(|(left, right)| {
                left.definition.definition_id == right.definition.definition_id
                    && left.definition.kind == right.definition.kind
                    && left.definition.column_ids == right.definition.column_ids
                    && left.definition.config_fingerprint == right.definition.config_fingerprint
                    && left.generation_id == right.generation_id
                    && left.freshness_policy == right.freshness_policy
            })
    }
}

impl SearchWritePlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.fulltext.is_empty() && self.sparse.is_empty()
    }
}
