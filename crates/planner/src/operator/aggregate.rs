// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Aggregate Operator
//!
//! Represents a GROUP BY and aggregation in the query plan.

pub use crate::binder::ir::GroupingSet;
use crate::expression::Expression;
use crate::operator::ColumnBinding;
use crate::plan::LogicalPlan;
use paro_common::types::LogicalType;
use paro_storage::statistics::BaseStatistics;

/// A correctness-proven functional dependency between GROUP BY expressions.
///
/// Indices address [`Aggregate::groups`]. The optimizer derives these facts
/// from declared keys and exact nullability; physical planning may use them to
/// choose a narrower lookup representation, but never has to rediscover the
/// underlying catalog proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDependency {
    pub determinants: Box<[usize]>,
    pub dependents: Box<[usize]>,
}

impl GroupDependency {
    /// Validate the positional proof before a physical representation relies
    /// on it. Dependencies must be non-empty, in bounds, unique, and disjoint.
    pub fn is_valid_for(&self, group_count: usize) -> bool {
        if self.determinants.is_empty() || self.dependents.is_empty() {
            return false;
        }
        let mut seen = std::collections::HashSet::with_capacity(
            self.determinants.len() + self.dependents.len(),
        );
        self.determinants
            .iter()
            .chain(self.dependents.iter())
            .all(|&index| index < group_count && seen.insert(index))
    }
}

/// Aggregate performs groupings and aggregate function evaluations.
#[derive(Debug)]
pub struct Aggregate {
    /// Table index for group output columns.
    pub group_index: usize,
    /// Table index for aggregate output columns.
    pub aggregate_index: usize,
    /// Table index for GROUPING() function outputs.
    pub groupings_index: usize,
    /// The input operator.
    pub child: Box<LogicalPlan>,
    /// GROUP BY expressions.
    pub groups: Vec<Expression>,
    /// GROUPING SETS metadata.
    pub grouping_sets: Vec<GroupingSet>,
    /// All aggregate expressions.
    pub aggregates: Vec<Expression>,
    /// Optional per-group statistics populated by optimizer later.
    pub group_stats: Vec<Option<BaseStatistics>>,
    /// Functional dependencies valid for this exact group-expression layout.
    pub group_dependencies: Vec<GroupDependency>,
    /// Types of the output columns (groups + aggregates + grouping functions).
    pub returned_types: Vec<LogicalType>,
    /// GROUPING() function definitions, each entry is a list of group indexes.
    pub grouping_functions: Vec<Vec<usize>>,
}

impl Aggregate {
    pub fn new(
        group_index: usize,
        aggregate_index: usize,
        groupings_index: usize,
        child: LogicalPlan,
        groups: Vec<Expression>,
        grouping_sets: Vec<GroupingSet>,
        aggregates: Vec<Expression>,
        grouping_functions: Vec<Vec<usize>>,
    ) -> Self {
        let mut aggregate = Self {
            group_index,
            aggregate_index,
            groupings_index,
            child: Box::new(child),
            group_stats: vec![None; groups.len()],
            group_dependencies: Vec::new(),
            groups,
            grouping_sets,
            aggregates,
            returned_types: Vec::new(),
            grouping_functions,
        };
        aggregate.recompute_returned_types();
        aggregate
    }

    pub fn recompute_returned_types(&mut self) {
        self.group_stats.resize(self.groups.len(), None);
        // Callers use this method after changing group expressions. Any prior
        // proof is positional and therefore stale until statistics propagation
        // derives it again over the settled logical tree.
        self.group_dependencies.clear();
        self.returned_types = self
            .groups
            .iter()
            .map(|expr| expr.return_type())
            .chain(self.aggregates.iter().map(|expr| expr.return_type()))
            .chain(self.grouping_functions.iter().map(|_| LogicalType::BigInt))
            .collect();
    }

    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        let mut bindings = Vec::with_capacity(
            self.groups.len() + self.aggregates.len() + self.grouping_functions.len(),
        );
        bindings
            .extend((0..self.groups.len()).map(|idx| ColumnBinding::new(self.group_index, idx)));
        bindings.extend(
            (0..self.aggregates.len()).map(|idx| ColumnBinding::new(self.aggregate_index, idx)),
        );
        bindings.extend(
            (0..self.grouping_functions.len())
                .map(|idx| ColumnBinding::new(self.groupings_index, idx)),
        );
        bindings
    }
}
