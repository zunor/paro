// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::CorrelatedColumnInfo;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelationBoundaryMode {
    ScopeBoundary,
    TransparentBoundary,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CorrelationSplit {
    pub local_to_child_parent: Vec<CorrelatedColumnInfo>,
    pub propagate_to_parent: Vec<CorrelatedColumnInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelationProjectionMode {
    IncludeAllPropagated,
    IncludeDepthOnePropagated,
}

impl CorrelationSplit {
    pub fn projected_correlations(
        &self,
        mode: CorrelationProjectionMode,
    ) -> Vec<CorrelatedColumnInfo> {
        let mut projected = Vec::new();
        let mut seen = HashSet::new();

        self.extend_unique(&mut projected, &mut seen, self.local_to_child_parent.iter());
        match mode {
            CorrelationProjectionMode::IncludeAllPropagated => {
                self.extend_unique(&mut projected, &mut seen, self.propagate_to_parent.iter())
            }
            CorrelationProjectionMode::IncludeDepthOnePropagated => self.extend_unique(
                &mut projected,
                &mut seen,
                self.propagate_to_parent
                    .iter()
                    .filter(|corr| corr.depth == 1),
            ),
        }

        projected
    }

    fn extend_unique<'a, I>(
        &self,
        projected: &mut Vec<CorrelatedColumnInfo>,
        seen: &mut HashSet<CorrelatedColumnInfo>,
        correlations: I,
    ) where
        I: IntoIterator<Item = &'a CorrelatedColumnInfo>,
    {
        for corr in correlations {
            if seen.insert(corr.clone()) {
                projected.push(corr.clone());
            }
        }
    }
}

pub fn split_child_correlated_columns(
    child_columns: Vec<CorrelatedColumnInfo>,
    mode: CorrelationBoundaryMode,
) -> CorrelationSplit {
    let mut split = CorrelationSplit::default();

    for corr in child_columns {
        match (mode, corr.depth) {
            (CorrelationBoundaryMode::ScopeBoundary, 0) => {}
            (CorrelationBoundaryMode::ScopeBoundary, 1) => split.local_to_child_parent.push(corr),
            (CorrelationBoundaryMode::ScopeBoundary, _) => {
                let mut propagated = corr;
                propagated.depth -= 1;
                split.propagate_to_parent.push(propagated);
            }
            (CorrelationBoundaryMode::TransparentBoundary, 0 | 1) => {
                split.propagate_to_parent.push(corr);
            }
            (CorrelationBoundaryMode::TransparentBoundary, _) => {
                let mut propagated = corr;
                propagated.depth -= 1;
                split.propagate_to_parent.push(propagated);
            }
        }
    }

    split
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;

    fn corr(depth: usize, table_index: usize) -> CorrelatedColumnInfo {
        CorrelatedColumnInfo {
            table_index,
            column_index: 0,
            return_type: LogicalType::Integer,
            name: format!("c{table_index}"),
            depth,
        }
    }

    #[test]
    fn scope_boundary_keeps_depth_one_local_and_decrements_outer_levels() {
        let split = split_child_correlated_columns(
            vec![corr(1, 10), corr(2, 20), corr(3, 30)],
            CorrelationBoundaryMode::ScopeBoundary,
        );

        assert_eq!(split.local_to_child_parent, vec![corr(1, 10)]);
        assert_eq!(split.propagate_to_parent, vec![corr(1, 20), corr(2, 30)]);
    }

    #[test]
    fn transparent_boundary_propagates_depth_one_without_consuming_it() {
        let split = split_child_correlated_columns(
            vec![corr(1, 10), corr(2, 20)],
            CorrelationBoundaryMode::TransparentBoundary,
        );

        assert!(split.local_to_child_parent.is_empty());
        assert_eq!(split.propagate_to_parent, vec![corr(1, 10), corr(1, 20)]);
    }

    #[test]
    fn projected_correlations_can_include_all_propagated_entries() {
        let split = CorrelationSplit {
            local_to_child_parent: vec![corr(1, 10)],
            propagate_to_parent: vec![corr(1, 20), corr(2, 30)],
        };

        assert_eq!(
            split.projected_correlations(CorrelationProjectionMode::IncludeAllPropagated),
            vec![corr(1, 10), corr(1, 20), corr(2, 30)]
        );
    }

    #[test]
    fn projected_correlations_can_limit_propagated_entries_to_depth_one() {
        let split = CorrelationSplit {
            local_to_child_parent: vec![corr(1, 10)],
            propagate_to_parent: vec![corr(1, 20), corr(2, 30)],
        };

        assert_eq!(
            split.projected_correlations(CorrelationProjectionMode::IncludeDepthOnePropagated),
            vec![corr(1, 10), corr(1, 20)]
        );
    }
}
