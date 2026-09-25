// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Borrowed scalar evidence for one selectivity model, before and after import.
//! Native operands stay in their arena; no executable expression is exported.

use super::*;
use crate::cascades::ids::{ColumnId, ScalarExprId};
use crate::cascades::scalar::{ComparisonOp, ScalarArena, ScalarKind};
use crate::cascades::BindingCatalog;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_storage::statistics::{BaseStatistics, EstimatedNumericDistribution};

/// Borrowed costing evidence, not an allocation containing a reconstructed
/// storage sketch. A ranking point and a value distribution are independent
/// facets; neither absence may erase the other. No field here proves an NDV
/// upper bound or a complete storage observation.
#[derive(Clone, Copy)]
pub(crate) struct ColumnPredicateEvidence<'a> {
    pub(crate) point: Option<u64>,
    pub(crate) values: Option<&'a BaseStatistics>,
    pub(crate) distribution: Option<EstimatedNumericDistribution>,
}

impl<'a> From<&'a ColumnStatistics> for ColumnPredicateEvidence<'a> {
    fn from(column: &'a ColumnStatistics) -> Self {
        Self {
            point: Some(column.distinct_evidence().point).filter(|point| *point != 0),
            values: Some(column.statistics()),
            distribution: column.estimated_numeric_distribution(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum PredicateKind<'a, Node> {
    Constant(&'a Value),
    Comparison(ComparisonType, Node, Node),
    And,
    Or,
    Operator(OperatorType),
    Function(Option<&'a BuiltinIntrinsicId>),
    Other,
}

/// The column coordinate here describes statistical dependence, not a
/// correctness proof of relational ownership. Each view resolves its own
/// typed operands and lexical scopes before exposing that coordinate.
pub(super) trait PredicateView<'a> {
    type Node: Copy;
    type Key: Copy + Eq + std::hash::Hash;

    fn key(&self, node: Self::Node) -> Self::Key;
    fn kind(&self, node: Self::Node) -> PredicateKind<'a, Self::Node>;
    fn children(&self, node: Self::Node, visit: impl FnMut(Self::Node));
    fn try_children<E>(
        &self,
        node: Self::Node,
        visit: impl FnMut(Self::Node) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E>;
    fn can_share(&self, node: Self::Node) -> bool;
    fn operator_child(&self, node: Self::Node, index: usize) -> Option<Self::Node>;
    fn operator_child_count(&self, node: Self::Node) -> usize;
    fn binding(&self, node: Self::Node) -> Option<ColumnBinding>;
    fn statistics(&self, node: Self::Node) -> Option<ColumnPredicateEvidence<'a>>;
    fn unresolved_column(&self, _node: Self::Node) -> bool {
        false
    }

    fn constant(&self, node: Self::Node) -> Option<&'a Value> {
        match self.kind(node) {
            PredicateKind::Constant(value) => Some(value),
            _ => None,
        }
    }

    fn single_binding(&self, node: Self::Node) -> Option<ColumnBinding> {
        self.scan_single_binding(node)
    }

    fn scan_single_binding(&self, node: Self::Node) -> Option<ColumnBinding> {
        let mut binding = None;
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(self.key(node)) {
                continue;
            }
            if self.unresolved_column(node) {
                return None;
            }
            if let Some(candidate) = self.binding(node) {
                if binding.is_some_and(|prior| prior != candidate) {
                    return None;
                }
                binding = Some(candidate);
            } else {
                self.children(node, |child| pending.push(child));
            }
        }
        binding
    }
}

impl<'a> PredicateView<'a> for StatisticsResolver<'a> {
    type Node = &'a Expression;
    type Key = paro_planner::expression::ExpressionIdentity;

    fn key(&self, node: Self::Node) -> Self::Key {
        node.allocation_identity()
    }

    fn kind(&self, node: Self::Node) -> PredicateKind<'a, Self::Node> {
        match node {
            Expression::Constant(value) => PredicateKind::Constant(&value.value),
            Expression::Comparison(comparison) => PredicateKind::Comparison(
                comparison.comparison_type,
                &comparison.left,
                &comparison.right,
            ),
            Expression::Conjunction(conjunction) => match conjunction.conjunction_type {
                ConjunctionType::And => PredicateKind::And,
                ConjunctionType::Or => PredicateKind::Or,
            },
            Expression::Operator(operator) => PredicateKind::Operator(operator.operator_type),
            Expression::Function(function) => PredicateKind::Function(function.builtin_intrinsic()),
            _ => PredicateKind::Other,
        }
    }

    fn children(&self, node: Self::Node, visit: impl FnMut(Self::Node)) {
        ExpressionIterator::enumerate_children(node, visit);
    }

    fn try_children<E>(
        &self,
        node: Self::Node,
        visit: impl FnMut(Self::Node) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        ExpressionIterator::try_enumerate_children(node, visit)
    }

    fn can_share(&self, node: Self::Node) -> bool {
        node.evaluation_properties().can_share_evaluation()
    }

    fn operator_child(&self, node: Self::Node, index: usize) -> Option<Self::Node> {
        let Expression::Operator(operator) = node else {
            return None;
        };
        operator.children.get(index)
    }

    fn operator_child_count(&self, node: Self::Node) -> usize {
        match node {
            Expression::Operator(operator) => operator.children.len(),
            _ => 0,
        }
    }

    fn binding(&self, node: Self::Node) -> Option<ColumnBinding> {
        StatisticsResolver::binding(self, node)
    }

    fn statistics(&self, node: Self::Node) -> Option<ColumnPredicateEvidence<'a>> {
        self.get(node).map(|column| column.as_ref().into())
    }

    fn unresolved_column(&self, node: Self::Node) -> bool {
        matches!(node, Expression::ColumnRef(column) if column.depth != 0)
            || matches!(node, Expression::Reference(_) if self.binding(node).is_none())
    }

    fn single_binding(&self, node: Self::Node) -> Option<ColumnBinding> {
        match self.kind(node) {
            PredicateKind::Constant(_) => None,
            PredicateKind::Comparison(comparison, left, right) => {
                if let Some((column, _, _)) =
                    column_constant_comparison(comparison, left, right, self)
                {
                    return self.binding(column);
                }
                self.scan_single_binding(node)
            }
            _ => self.scan_single_binding(node),
        }
    }
}

pub(crate) struct NativePredicateView<'a, F> {
    pub(crate) arena: &'a ScalarArena,
    pub(crate) bindings: &'a BindingCatalog,
    pub(crate) column_stats: F,
}

impl<'a, F: Fn(ColumnId) -> Option<ColumnPredicateEvidence<'a>>> PredicateView<'a>
    for NativePredicateView<'a, F>
{
    type Node = ScalarExprId;
    type Key = ScalarExprId;

    fn key(&self, node: Self::Node) -> Self::Key {
        node
    }

    fn kind(&self, node: Self::Node) -> PredicateKind<'a, Self::Node> {
        let node = self
            .arena
            .get(node)
            .expect("selectivity root belongs to its immutable arena");
        match &node.kind {
            ScalarKind::Constant { value } => PredicateKind::Constant(value.value()),
            ScalarKind::Comparison(comparison) => PredicateKind::Comparison(
                match comparison {
                    ComparisonOp::Equal => ComparisonType::Equal,
                    ComparisonOp::NotEqual => ComparisonType::NotEqual,
                    ComparisonOp::Less => ComparisonType::LessThan,
                    ComparisonOp::LessOrEqual => ComparisonType::LessThanOrEqual,
                    ComparisonOp::Greater => ComparisonType::GreaterThan,
                    ComparisonOp::GreaterOrEqual => ComparisonType::GreaterThanOrEqual,
                    ComparisonOp::DistinctFrom => ComparisonType::DistinctFrom,
                    ComparisonOp::NotDistinctFrom => ComparisonType::NotDistinctFrom,
                },
                node.children[0],
                node.children[1],
            ),
            ScalarKind::And => PredicateKind::And,
            ScalarKind::Or => PredicateKind::Or,
            ScalarKind::Operator { operator } => PredicateKind::Operator(*operator),
            ScalarKind::Function { routine } => {
                PredicateKind::Function(routine.routine_meta().and_then(
                    |meta| match &meta.identity {
                        RoutineCallIdentity::Builtin { intrinsic, .. } => Some(intrinsic),
                        RoutineCallIdentity::Catalog { .. } => None,
                    },
                ))
            }
            _ => PredicateKind::Other,
        }
    }

    fn children(&self, node: Self::Node, mut visit: impl FnMut(Self::Node)) {
        if let Some(node) = self.arena.get(node) {
            for &child in &node.children {
                visit(child);
            }
        }
    }

    fn try_children<E>(
        &self,
        node: Self::Node,
        mut visit: impl FnMut(Self::Node) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if let Some(node) = self.arena.get(node) {
            for &child in &node.children {
                visit(child)?;
            }
        }
        Ok(())
    }

    fn can_share(&self, node: Self::Node) -> bool {
        self.arena
            .get(node)
            .is_some_and(|node| node.properties.can_repeat_evaluation())
    }

    fn operator_child(&self, node: Self::Node, index: usize) -> Option<Self::Node> {
        let node = self.arena.get(node)?;
        matches!(node.kind, ScalarKind::Operator { .. })
            .then(|| node.children.get(index).copied())
            .flatten()
    }

    fn operator_child_count(&self, node: Self::Node) -> usize {
        self.arena.get(node).map_or(0, |node| node.children.len())
    }

    fn binding(&self, node: Self::Node) -> Option<ColumnBinding> {
        let ScalarKind::Column(column) = self.arena.get(node)?.kind else {
            return None;
        };
        self.bindings.relation_binding(column)
    }

    fn statistics(&self, node: Self::Node) -> Option<ColumnPredicateEvidence<'a>> {
        let ScalarKind::Column(column) = self.arena.get(node)?.kind else {
            return None;
        };
        (self.column_stats)(column)
    }

    fn single_binding(&self, node: Self::Node) -> Option<ColumnBinding> {
        let node = self.arena.get(node)?;
        if node.properties.has_outer_references() {
            return None;
        }
        let mut columns = node.properties.local_columns();
        let column = columns.next()?;
        if columns.next().is_some() {
            return None;
        }
        self.bindings.relation_binding(column)
    }
}
