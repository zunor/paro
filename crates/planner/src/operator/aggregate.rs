// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Aggregate Operator
//!
//! Represents a GROUP BY and aggregation in the query plan.

pub use crate::binder::ir::GroupingSet;
use crate::expression::{AggregateType, Expression, ExpressionIterator, ExpressionVisitDecision};
use crate::operator::ColumnBinding;
use crate::plan::LogicalPlan;
use paro_common::error::{self as paro_error, Result};
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

/// One scalar reduction over the finalized outputs of a grouped aggregate.
///
/// This annotation does not add SQL-visible columns to [`Aggregate`]. Instead,
/// `reduction_index` names the hidden outputs of `scalar_expressions` so the
/// predicate can compare every original aggregate row with the one-time scalar
/// result. Its expression fields intentionally use three separate domains:
///
/// - reducer arguments are `ColumnRef(aggregate_index, aggregate_ordinal)`;
/// - scalar expressions use local `Reference(reducer_ordinal)` values;
/// - the predicate uses `ColumnRef` values from `aggregate_index` and
///   `reduction_index` only.
///
/// Physical planning erases these logical bindings and rebases the three
/// domains to positional references. The hidden scalar outputs remain owned by
/// the aggregate and never enter its public bindings, types, or output names.
#[derive(Debug, Clone)]
pub struct PostAggregateReduction {
    /// Hidden binding for the final scalar-expression outputs.
    pub reduction_index: usize,
    /// Aggregates over the finalized aggregate-only output chunk.
    pub reducers: Vec<Expression>,
    /// Scalar expressions evaluated once over reducer outputs.
    pub scalar_expressions: Vec<Expression>,
    /// Per-group predicate over original aggregate values and scalar outputs.
    pub predicate: Expression,
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
    /// Optional one-time scalar reduction over finalized aggregate values.
    /// Its outputs are hidden implementation values, not SQL-visible columns.
    pub post_reduction: Option<PostAggregateReduction>,
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
            post_reduction: None,
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

    pub fn with_post_reduction(mut self, post_reduction: PostAggregateReduction) -> Self {
        self.post_reduction = Some(post_reduction);
        self
    }

    /// Verify the correctness-bearing local expression domains of the optional
    /// post-aggregate reduction annotation.
    pub fn verify_post_reduction(&self) -> Result<()> {
        let Some(reduction) = &self.post_reduction else {
            return Ok(());
        };
        if reduction.reduction_index == self.group_index
            || reduction.reduction_index == self.aggregate_index
            || reduction.reduction_index == self.groupings_index
        {
            return Err(paro_error::internal(
                "Post-aggregate reduction must own an independent table index".to_string(),
            ));
        }
        let plain_grouping_domain = !self.groups.is_empty()
            && self.grouping_functions.is_empty()
            && (self.grouping_sets.is_empty()
                || (self.grouping_sets.len() == 1
                    && self.grouping_sets[0].expressions.len() == self.groups.len()
                    && self.grouping_sets[0]
                        .expressions
                        .iter()
                        .copied()
                        .collect::<std::collections::HashSet<_>>()
                        == (0..self.groups.len()).collect()));
        if !plain_grouping_domain {
            return Err(paro_error::internal(
                "Post-aggregate reduction requires one plain, non-empty grouping domain"
                    .to_string(),
            ));
        }
        if self.aggregates.is_empty()
            || reduction.reducers.is_empty()
            || reduction.scalar_expressions.is_empty()
        {
            return Err(paro_error::internal(
                "Post-aggregate reduction requires aggregate inputs, reducers, and scalar expressions"
                    .to_string(),
            ));
        }

        let aggregate_types = self
            .aggregates
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>();
        let mut reducer_types = Vec::with_capacity(reduction.reducers.len());
        for (reducer_idx, reducer) in reduction.reducers.iter().enumerate() {
            let Expression::Aggregate(reducer) = reducer else {
                return Err(paro_error::internal(format!(
                    "Post-aggregate reducer {reducer_idx} is not an aggregate expression"
                )));
            };
            if reducer.aggr_type != AggregateType::NonDistinct
                || reducer.filter.is_some()
                || !reducer.order_bys.is_empty()
                || reducer.children.is_empty()
            {
                return Err(paro_error::internal(format!(
                    "Post-aggregate reducer {reducer_idx} must be non-distinct, unfiltered, unordered, and consume finalized aggregate values"
                )));
            }
            if reducer.function.destructor.is_some() {
                return Err(paro_error::internal(format!(
                    "Post-aggregate reducer {reducer_idx} must use inline-owned state"
                )));
            }
            if reducer.return_type != reducer.function.return_type {
                return Err(paro_error::internal(format!(
                    "Post-aggregate reducer {reducer_idx} return type does not match its bound function"
                )));
            }
            if reducer.children.len() != reducer.function.arguments.len() {
                return Err(paro_error::internal(format!(
                    "Post-aggregate reducer {reducer_idx} argument count does not match its bound function"
                )));
            }
            for (child_idx, child) in reducer.children.iter().enumerate() {
                let Expression::ColumnRef(column) = child else {
                    return Err(paro_error::internal(format!(
                        "Post-aggregate reducer {reducer_idx} argument {child_idx} must directly reference a finalized aggregate output"
                    )));
                };
                if column.depth != 0 || column.binding.table_index != self.aggregate_index {
                    return Err(paro_error::internal(format!(
                        "Post-aggregate reducer {reducer_idx} argument {child_idx} references a non-aggregate binding"
                    )));
                }
                let Some(expected_type) = aggregate_types.get(column.binding.column_index) else {
                    return Err(paro_error::internal(format!(
                        "Post-aggregate reducer {reducer_idx} argument {child_idx} is out of bounds"
                    )));
                };
                if &column.return_type != expected_type {
                    return Err(paro_error::internal(format!(
                        "Post-aggregate reducer {reducer_idx} argument {child_idx} type mismatch: expected={expected_type:?}, actual={:?}",
                        column.return_type
                    )));
                }
                if reducer.function.arguments.get(child_idx) != Some(&column.return_type) {
                    return Err(paro_error::internal(format!(
                        "Post-aggregate reducer {reducer_idx} argument {child_idx} type does not match its bound function"
                    )));
                }
            }
            reducer_types.push(reducer.return_type.clone());
        }

        let scalar_types = reduction
            .scalar_expressions
            .iter()
            .enumerate()
            .map(|(scalar_idx, expression)| {
                verify_local_scalar_expression(expression, &reducer_types, scalar_idx)?;
                Ok(expression.return_type())
            })
            .collect::<Result<Vec<_>>>()?;

        if reduction.predicate.return_type() != LogicalType::Boolean {
            return Err(paro_error::internal(
                "Post-aggregate reduction predicate must return Boolean".to_string(),
            ));
        }
        let properties = reduction.predicate.evaluation_properties();
        if !properties.can_share_evaluation() || properties.is_reorder_fence() {
            return Err(paro_error::internal(
                "Post-aggregate reduction predicate must be deterministic, side-effect free, and native"
                    .to_string(),
            ));
        }

        let mut saw_reduction_output = false;
        let mut error = None;
        ExpressionIterator::visit(&reduction.predicate, &mut |node| {
            if error.is_some() {
                return ExpressionVisitDecision::SkipChildren;
            }
            match node {
                Expression::ColumnRef(column) => {
                    let source_types = if column.binding.table_index == self.aggregate_index {
                        &aggregate_types
                    } else if column.binding.table_index == reduction.reduction_index {
                        saw_reduction_output = true;
                        &scalar_types
                    } else {
                        error = Some(paro_error::internal(format!(
                            "Post-aggregate reduction predicate references unavailable binding {:?}",
                            column.binding
                        )));
                        return ExpressionVisitDecision::SkipChildren;
                    };
                    if column.depth != 0 {
                        error = Some(paro_error::internal(
                            "Post-aggregate reduction predicate cannot contain correlated columns"
                                .to_string(),
                        ));
                    } else if source_types.get(column.binding.column_index)
                        != Some(&column.return_type)
                    {
                        error = Some(paro_error::internal(format!(
                            "Post-aggregate reduction predicate binding {:?} has an invalid type",
                            column.binding
                        )));
                    }
                    ExpressionVisitDecision::SkipChildren
                }
                Expression::Aggregate(_)
                | Expression::Reference(_)
                | Expression::Subquery(_)
                | Expression::Window(_) => {
                    error = Some(paro_error::internal(
                        "Post-aggregate reduction predicate contains an invalid expression domain"
                            .to_string(),
                    ));
                    ExpressionVisitDecision::SkipChildren
                }
                _ => ExpressionVisitDecision::Descend,
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        if !saw_reduction_output {
            return Err(paro_error::internal(
                "Post-aggregate reduction predicate must reference a scalar reduction output"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

fn verify_local_scalar_expression(
    expression: &Expression,
    reducer_types: &[LogicalType],
    scalar_idx: usize,
) -> Result<()> {
    let properties = expression.evaluation_properties();
    if !properties.can_share_evaluation() || properties.is_reorder_fence() {
        return Err(paro_error::internal(format!(
            "Post-aggregate scalar expression {scalar_idx} must be deterministic, side-effect free, and native"
        )));
    }

    let mut error = None;
    ExpressionIterator::visit(expression, &mut |node| {
        if error.is_some() {
            return ExpressionVisitDecision::SkipChildren;
        }
        match node {
            Expression::Reference(reference) => {
                if reducer_types.get(reference.index) != Some(&reference.return_type) {
                    error = Some(paro_error::internal(format!(
                        "Post-aggregate scalar expression {scalar_idx} has an invalid reducer reference {}",
                        reference.index
                    )));
                }
                ExpressionVisitDecision::SkipChildren
            }
            Expression::Aggregate(_)
            | Expression::ColumnRef(_)
            | Expression::Subquery(_)
            | Expression::Window(_) => {
                error = Some(paro_error::internal(format!(
                    "Post-aggregate scalar expression {scalar_idx} contains an invalid expression domain"
                )));
                ExpressionVisitDecision::SkipChildren
            }
            _ => ExpressionVisitDecision::Descend,
        }
    });
    error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{
        AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
        ReferenceExpression,
    };
    use crate::operator::ExpressionGet;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::aggregate::distributive::minmax::get_max_function;

    fn aggregate_with_reduction() -> Aggregate {
        let child = LogicalPlan::synthetic(crate::operator::LogicalOperator::ExpressionGet(
            ExpressionGet::new(
                0,
                Vec::new(),
                vec!["key".to_string()],
                vec![LogicalType::Integer],
            ),
        ));
        let count = Expression::Aggregate(AggregateExpression::new(
            get_count_star_function(),
            Vec::new(),
            LogicalType::BigInt,
        ));
        let (max, _) = get_max_function()
            .bind(&[LogicalType::BigInt])
            .expect("bind max(bigint)");
        let reducer = Expression::Aggregate(AggregateExpression::new(
            max,
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(2, 0),
                LogicalType::BigInt,
            ))],
            LogicalType::BigInt,
        ));
        let predicate = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(2, 0),
                LogicalType::BigInt,
            )),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(4, 0),
                LogicalType::BigInt,
            )),
        ));
        Aggregate::new(
            1,
            2,
            3,
            child,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            Vec::new(),
            vec![count],
            Vec::new(),
        )
        .with_post_reduction(PostAggregateReduction {
            reduction_index: 4,
            reducers: vec![reducer],
            scalar_expressions: vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::BigInt,
            ))],
            predicate,
        })
    }

    #[test]
    fn post_reduction_has_validated_hidden_binding_domains() {
        let aggregate = aggregate_with_reduction();
        aggregate
            .verify_post_reduction()
            .expect("valid reduction annotation");
        assert_eq!(aggregate.returned_types.len(), 2);
        assert_eq!(aggregate.get_column_bindings().len(), 2);
        assert_eq!(
            aggregate
                .post_reduction
                .as_ref()
                .expect("reduction")
                .reduction_index,
            4
        );
    }

    #[test]
    fn post_reduction_rejects_invalid_local_scalar_reference() {
        let mut aggregate = aggregate_with_reduction();
        aggregate
            .post_reduction
            .as_mut()
            .expect("reduction")
            .scalar_expressions[0] =
            Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt));

        let error = aggregate
            .verify_post_reduction()
            .expect_err("invalid local reducer reference must fail");
        assert!(error.to_string().contains("invalid reducer reference"));
    }

    #[test]
    fn post_reduction_rejects_multiple_grouping_domains() {
        let mut aggregate = aggregate_with_reduction();
        aggregate.grouping_sets = vec![
            GroupingSet {
                expressions: vec![0],
            },
            GroupingSet {
                expressions: Vec::new(),
            },
        ];

        let error = aggregate
            .verify_post_reduction()
            .expect_err("multiple grouping levels must not feed one scalar reduction");
        assert!(error.to_string().contains("one plain, non-empty grouping"));
    }
}
