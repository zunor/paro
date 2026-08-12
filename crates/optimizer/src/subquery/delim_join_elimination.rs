// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Eliminate redundant joins against `DelimGet`.

use std::collections::HashSet;

use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonType, ConjunctionType, Expression, ExpressionIterator,
    OperatorExpression, OperatorType,
};
use paro_planner::operator::{
    ComparisonJoin, Filter, Join, JoinComparisonType, JoinCondition, JoinType, LogicalOperator,
};
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

use crate::expression::binding_replacer::{ColumnBindingReplacer, ReplacementBinding};

/// Remove redundant joins against `DelimGet`.
pub struct DelimJoinElimination;

impl DelimJoinElimination {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_recursive_plan(plan)
    }

    fn optimize_recursive_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_recursive_plan(child));
        plan.map_operator(|operator| self.optimize_operator(operator))
    }

    fn optimize_operator(&mut self, plan: LogicalOperator) -> LogicalOperator {
        let LogicalOperator::Join(Join::Comparison(mut join)) = plan else {
            return plan;
        };
        if join.duplicate_eliminated_columns.is_empty() {
            return LogicalOperator::Join(Join::Comparison(join));
        }

        if Self::can_decorrelate_existence(&join) {
            return LogicalOperator::Join(Join::Comparison(Self::decorrelate_existence(join)));
        }

        let mut replacements = Vec::new();
        {
            let delim_side = if join.delim_flipped {
                &mut join.left
            } else {
                &mut join.right
            };
            while let Some(mut current) =
                Self::remove_first_redundant_join(&mut delim_side.operator)
            {
                replacements.append(&mut current);
            }
        }

        if replacements.is_empty() {
            return LogicalOperator::Join(Join::Comparison(join));
        }

        let mut op = LogicalOperator::Join(Join::Comparison(join));
        let mut replacer = ColumnBindingReplacer::new();
        replacer.replacement_bindings = replacements;
        replacer.visit_operator(&mut op);

        let mut join = match op {
            LogicalOperator::Join(Join::Comparison(join)) => join,
            other => return other,
        };
        let delim_side = if join.delim_flipped {
            &join.left
        } else {
            &join.right
        };
        if !Self::contains_delim_get(delim_side.as_ref()) {
            join.duplicate_eliminated_columns.clear();
            join.delim_flipped = false;
        }
        LogicalOperator::Join(Join::Comparison(join))
    }

    /// Collapse the canonical correlated-EXISTS shape into one comparison
    /// join. The delimiter cross product only supplies the current outer
    /// correlation tuple; once the marker is consumed as SEMI/ANTI, the same
    /// tuple is already available directly from the preserved side.
    ///
    /// ```text
    /// outer SEMI/ANTI projection(filter(base CROSS delim))
    ///   -> outer SEMI/ANTI base
    /// ```
    ///
    /// Correlation predicates are rebound from delim columns to their outer
    /// expressions. This removes both delimiter materialization and the second
    /// hash table while preserving arbitrary side-local work below `base`.
    fn can_decorrelate_existence(join: &ComparisonJoin) -> bool {
        if !matches!(join.join_type, JoinType::Semi | JoinType::Anti)
            || join.delim_flipped
            || join.mark_index.is_some()
            || join.mark_null_condition_start.is_some()
            || join.anti_join_mode != paro_planner::operator::AntiJoinMode::Regular
            || join.conditions.len() != join.duplicate_eliminated_columns.len()
            || join
                .conditions
                .iter()
                .any(|condition| condition.comparison != JoinComparisonType::NotDistinctFrom)
        {
            return false;
        }

        let Some(filter_plan) = passive_projection_child(join.right.as_ref()) else {
            return false;
        };
        let LogicalOperator::Filter(filter) = &filter_plan.operator else {
            return false;
        };
        let (delim, base, correlated_join_conditions) = match &filter.child.operator {
            LogicalOperator::Join(Join::Cross(cross)) => {
                match (&cross.left.operator, &cross.right.operator) {
                    (LogicalOperator::DelimGet(delim), _) => (delim, cross.right.as_ref(), None),
                    (_, LogicalOperator::DelimGet(delim)) => (delim, cross.left.as_ref(), None),
                    _ => return false,
                }
            }
            LogicalOperator::Join(Join::Comparison(correlated_join))
                if correlated_join.join_type == JoinType::Inner
                    && correlated_join.duplicate_eliminated_columns.is_empty() =>
            {
                match (
                    &correlated_join.left.operator,
                    &correlated_join.right.operator,
                ) {
                    (LogicalOperator::DelimGet(delim), _) => (
                        delim,
                        correlated_join.right.as_ref(),
                        Some(correlated_join.conditions.as_slice()),
                    ),
                    (_, LogicalOperator::DelimGet(delim)) => (
                        delim,
                        correlated_join.left.as_ref(),
                        Some(correlated_join.conditions.as_slice()),
                    ),
                    _ => return false,
                }
            }
            _ => return false,
        };
        if delim.chunk_types.len() != join.duplicate_eliminated_columns.len() {
            return false;
        }
        if !outer_conditions_bind_exact_delim_columns(join, delim.table_index) {
            return false;
        }
        let base_bindings = base
            .get_column_bindings()
            .into_iter()
            .collect::<HashSet<_>>();
        let join_conditions_supported = correlated_join_conditions.is_none_or(|conditions| {
            conditions.iter().all(|condition| {
                correlated_condition_from_parts(
                    condition.left.clone(),
                    condition.right.clone(),
                    join_to_expression_comparison(condition.comparison),
                    delim.table_index,
                    &join.duplicate_eliminated_columns,
                )
                .is_some_and(|condition| {
                    expression_references_only_bindings(&condition.right, &base_bindings)
                })
            })
        });

        let filters_supported =
            filter
                .expressions
                .iter()
                .flat_map(conjunction_terms)
                .all(|expression| {
                    correlated_join_condition(
                        expression.clone(),
                        delim.table_index,
                        &join.duplicate_eliminated_columns,
                    )
                    .is_some_and(|condition| {
                        expression_references_only_bindings(&condition.right, &base_bindings)
                    }) || (!expression_references_table(expression, delim.table_index)
                        && expression_references_only_bindings(expression, &base_bindings)
                        && !expression.evaluation_properties().is_reorder_fence())
                });
        filters_supported && join_conditions_supported
    }

    fn decorrelate_existence(mut join: ComparisonJoin) -> ComparisonJoin {
        let right = *std::mem::replace(
            &mut join.right,
            Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
        );
        let mut current = right;
        while matches!(current.operator, LogicalOperator::Projection(_)) {
            let LogicalOperator::Projection(projection) = current.operator else {
                unreachable!()
            };
            current = *projection.child;
        }
        let LogicalOperator::Filter(filter) = current.operator else {
            unreachable!("validated correlated existence filter")
        };
        let mut conditions = Vec::new();
        let (base, delim_table_index) = match filter.child.operator {
            LogicalOperator::Join(Join::Cross(cross)) => {
                match (cross.left.operator, cross.right.operator) {
                    (LogicalOperator::DelimGet(delim), right) => (
                        LogicalPlan {
                            id: cross.right.id,
                            stats: cross.right.stats,
                            operator: right,
                        },
                        delim.table_index,
                    ),
                    (left, LogicalOperator::DelimGet(delim)) => (
                        LogicalPlan {
                            id: cross.left.id,
                            stats: cross.left.stats,
                            operator: left,
                        },
                        delim.table_index,
                    ),
                    _ => unreachable!("validated correlated existence delimiter"),
                }
            }
            LogicalOperator::Join(Join::Comparison(correlated_join)) => {
                let (base, delim_table_index) = match (
                    &correlated_join.left.operator,
                    &correlated_join.right.operator,
                ) {
                    (LogicalOperator::DelimGet(delim), _) => {
                        (*correlated_join.right, delim.table_index)
                    }
                    (_, LogicalOperator::DelimGet(delim)) => {
                        (*correlated_join.left, delim.table_index)
                    }
                    _ => unreachable!("validated correlated existence delimiter"),
                };
                for condition in correlated_join.conditions {
                    conditions.push(
                        correlated_condition_from_parts(
                            condition.left,
                            condition.right,
                            join_to_expression_comparison(condition.comparison),
                            delim_table_index,
                            &join.duplicate_eliminated_columns,
                        )
                        .expect("validated correlated existence join condition"),
                    );
                }
                (base, delim_table_index)
            }
            _ => unreachable!("validated correlated existence join"),
        };
        let mut local_filters = Vec::new();
        for expression in filter.expressions {
            for term in into_conjunction_terms(expression) {
                match correlated_join_condition(
                    term.clone(),
                    delim_table_index,
                    &join.duplicate_eliminated_columns,
                ) {
                    Some(condition) => conditions.push(condition),
                    None => local_filters.push(term),
                }
            }
        }
        let base = if local_filters.is_empty() {
            base
        } else {
            LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(base, local_filters)))
        };

        let mut direct = ComparisonJoin::new(join.join_type, *join.left, base, conditions);
        direct.anti_join_mode = join.anti_join_mode;
        direct.left_projection_map = join.left_projection_map;
        direct
    }

    fn remove_first_redundant_join(node: &mut LogicalOperator) -> Option<Vec<ReplacementBinding>> {
        match node {
            LogicalOperator::Join(Join::Comparison(join)) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.left.operator)
                {
                    return Some(replacements);
                }
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.right.operator)
                {
                    return Some(replacements);
                }
                Self::try_remove_join_with_delim_get(node)
            }
            LogicalOperator::Join(Join::Any(join)) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.left.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut join.right.operator)
            }
            LogicalOperator::Join(Join::Cross(join)) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut join.left.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut join.right.operator)
            }
            LogicalOperator::Filter(filter) => {
                Self::remove_first_redundant_join(&mut filter.child.operator)
            }
            LogicalOperator::Projection(proj) => {
                Self::remove_first_redundant_join(&mut proj.child.operator)
            }
            LogicalOperator::ExternalProject(project) => {
                Self::remove_first_redundant_join(&mut project.child.operator)
            }
            LogicalOperator::ExternalTable(table) => table
                .child
                .as_mut()
                .and_then(|child| Self::remove_first_redundant_join(&mut child.operator)),
            LogicalOperator::Limit(limit) => {
                Self::remove_first_redundant_join(&mut limit.child.operator)
            }
            LogicalOperator::Order(order) => {
                Self::remove_first_redundant_join(&mut order.child.operator)
            }
            LogicalOperator::TopN(topn) => {
                Self::remove_first_redundant_join(&mut topn.child.operator)
            }
            LogicalOperator::Aggregate(agg) => {
                Self::remove_first_redundant_join(&mut agg.child.operator)
            }
            LogicalOperator::Distinct(distinct) => {
                Self::remove_first_redundant_join(&mut distinct.child.operator)
            }
            LogicalOperator::Window(window) => {
                Self::remove_first_redundant_join(&mut window.child.operator)
            }
            LogicalOperator::Explain(explain) => {
                Self::remove_first_redundant_join(&mut explain.child.operator)
            }
            LogicalOperator::SetOperation(setop) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut setop.left.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut setop.right.operator)
            }
            LogicalOperator::MaterializedCTE(cte) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut cte.cte_query.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut cte.child.operator)
            }
            LogicalOperator::RecursiveCTE(cte) => {
                if let Some(replacements) =
                    Self::remove_first_redundant_join(&mut cte.anchor.operator)
                {
                    return Some(replacements);
                }
                Self::remove_first_redundant_join(&mut cte.recursive.operator)
            }
            LogicalOperator::Delete(delete) => {
                Self::remove_first_redundant_join(&mut delete.child.operator)
            }
            LogicalOperator::Update(update) => {
                Self::remove_first_redundant_join(&mut update.child.operator)
            }
            LogicalOperator::Insert(insert) => {
                Self::remove_first_redundant_join(&mut insert.child.operator)
            }
            LogicalOperator::CopyTo(copy) => {
                Self::remove_first_redundant_join(&mut copy.child.operator)
            }
            LogicalOperator::EmptyResult(empty) => {
                Self::remove_first_redundant_join(&mut empty.child.operator)
            }
            LogicalOperator::GraphExpand(expand) => {
                Self::remove_first_redundant_join(&mut expand.child.operator)
            }
            LogicalOperator::Get(_)
            | LogicalOperator::ExpressionGet(_)
            | LogicalOperator::DelimGet(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::TableFunctionGet(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
            | LogicalOperator::DummyScan => None,
        }
    }

    fn try_remove_join_with_delim_get(
        node: &mut LogicalOperator,
    ) -> Option<Vec<ReplacementBinding>> {
        let LogicalOperator::Join(Join::Comparison(join)) = node else {
            return None;
        };
        if !matches!(join.join_type, JoinType::Inner | JoinType::Semi) {
            return None;
        }

        let delim_idx = if Self::operator_is_delim_get(&join.left.operator) {
            0
        } else if Self::operator_is_delim_get(&join.right.operator) {
            1
        } else {
            return None;
        };

        let (delim_table_index, delim_types, mut filter_expressions) = if delim_idx == 0 {
            Self::extract_delim_metadata(join.left.as_ref())?
        } else {
            Self::extract_delim_metadata(join.right.as_ref())?
        };

        if join.conditions.len() != delim_types.len() {
            return None;
        }

        let mut replacements = Vec::with_capacity(join.conditions.len());
        for cond in &join.conditions {
            let (delim_expr, other_expr) = if delim_idx == 0 {
                (&cond.left, &cond.right)
            } else {
                (&cond.right, &cond.left)
            };

            let Expression::ColumnRef(delim_colref) = delim_expr else {
                return None;
            };
            let Expression::ColumnRef(other_colref) = other_expr else {
                return None;
            };
            if delim_colref.binding.table_index != delim_table_index {
                return None;
            }

            replacements.push(ReplacementBinding::new(
                delim_colref.binding,
                other_colref.binding,
            ));

            if !matches!(
                cond.comparison,
                paro_planner::operator::JoinComparisonType::NotDistinctFrom
                    | paro_planner::operator::JoinComparisonType::DistinctFrom
            ) {
                filter_expressions.push(Expression::Operator(OperatorExpression::new_unary(
                    OperatorType::IsNotNull,
                    Expression::ColumnRef(ColumnRefExpression::new(
                        other_colref.binding,
                        other_colref.return_type.clone(),
                    )),
                    LogicalType::Boolean,
                )));
            }
        }

        let replacement_plan = if delim_idx == 0 {
            std::mem::replace(
                &mut *join.right,
                LogicalPlan::synthetic(LogicalOperator::DummyScan),
            )
        } else {
            std::mem::replace(
                &mut *join.left,
                LogicalPlan::synthetic(LogicalOperator::DummyScan),
            )
        };

        *node = if filter_expressions.is_empty() {
            replacement_plan.operator
        } else {
            LogicalOperator::Filter(Filter::new(replacement_plan, filter_expressions))
        };
        Some(replacements)
    }

    fn operator_is_delim_get(op: &LogicalOperator) -> bool {
        matches!(op, LogicalOperator::DelimGet(_))
            || matches!(op, LogicalOperator::Filter(filter) if matches!(&filter.child.operator, LogicalOperator::DelimGet(_)))
    }

    fn extract_delim_metadata(
        plan: &LogicalPlan,
    ) -> Option<(usize, Vec<LogicalType>, Vec<Expression>)> {
        match &plan.operator {
            LogicalOperator::DelimGet(delim_get) => Some((
                delim_get.table_index,
                delim_get.chunk_types.clone(),
                Vec::new(),
            )),
            LogicalOperator::Filter(filter) => {
                let LogicalOperator::DelimGet(delim_get) = &filter.child.operator else {
                    return None;
                };
                Some((
                    delim_get.table_index,
                    delim_get.chunk_types.clone(),
                    filter.expressions.clone(),
                ))
            }
            _ => None,
        }
    }

    fn contains_delim_get(plan: &LogicalPlan) -> bool {
        if Self::operator_is_delim_get(&plan.operator) {
            return true;
        }
        plan.children()
            .iter()
            .any(|child| Self::contains_delim_get(child))
    }
}

fn conjunction_terms(expression: &Expression) -> Vec<&Expression> {
    match expression {
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::And =>
        {
            conjunction
                .children
                .iter()
                .flat_map(conjunction_terms)
                .collect()
        }
        _ => vec![expression],
    }
}

fn passive_projection_child(mut plan: &LogicalPlan) -> Option<&LogicalPlan> {
    while let LogicalOperator::Projection(projection) = &plan.operator {
        if !projection
            .expressions
            .iter()
            .all(Expression::is_passive_value)
        {
            return None;
        }
        plan = projection.child.as_ref();
    }
    Some(plan)
}

/// Verify that the delimiter join's outer equality conditions are the exact
/// identity map installed by dependent-join flattening. Merely checking their
/// count and comparison kind is insufficient: a business predicate with the
/// same arity must never authorize removal of the delimiter.
fn outer_conditions_bind_exact_delim_columns(
    join: &ComparisonJoin,
    delim_table_index: usize,
) -> bool {
    let mut seen = vec![false; join.duplicate_eliminated_columns.len()];
    for condition in &join.conditions {
        let left_delim = resolve_projected_delim_column(
            join.right.as_ref(),
            condition.left.clone(),
            delim_table_index,
        );
        let right_delim = resolve_projected_delim_column(
            join.right.as_ref(),
            condition.right.clone(),
            delim_table_index,
        );
        let (delim_column, outer_expression) = match (left_delim, right_delim) {
            (Some(column), None) => (column, &condition.right),
            (None, Some(column)) => (column, &condition.left),
            _ => return false,
        };
        let Some(expected_outer) = join.duplicate_eliminated_columns.get(delim_column) else {
            return false;
        };
        if seen[delim_column] || !outer_expression.equals(expected_outer) {
            return false;
        }
        seen[delim_column] = true;
    }
    seen.into_iter().all(|matched| matched)
}

fn resolve_projected_delim_column(
    mut plan: &LogicalPlan,
    mut expression: Expression,
    delim_table_index: usize,
) -> Option<usize> {
    while let LogicalOperator::Projection(projection) = &plan.operator {
        let Expression::ColumnRef(column) = &expression else {
            return None;
        };
        if column.depth != 0 || column.binding.table_index != projection.table_index {
            return None;
        }
        expression = projection
            .expressions
            .get(column.binding.column_index)?
            .clone();
        plan = projection.child.as_ref();
    }
    let Expression::ColumnRef(column) = expression else {
        return None;
    };
    (column.depth == 0 && column.binding.table_index == delim_table_index)
        .then_some(column.binding.column_index)
}

fn into_conjunction_terms(expression: Expression) -> Vec<Expression> {
    match expression {
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::And =>
        {
            conjunction
                .children
                .into_iter()
                .flat_map(into_conjunction_terms)
                .collect()
        }
        _ => vec![expression],
    }
}

fn expression_references_only_table(expression: &Expression, table_index: usize) -> bool {
    let mut found = false;
    let mut foreign = false;
    fn visit(expression: &Expression, table_index: usize, found: &mut bool, foreign: &mut bool) {
        if let Expression::ColumnRef(column) = expression {
            if column.depth != 0 || column.binding.table_index != table_index {
                *foreign = true;
            } else {
                *found = true;
            }
            return;
        }
        ExpressionIterator::enumerate_children(expression, |child| {
            visit(child, table_index, found, foreign)
        });
    }
    visit(expression, table_index, &mut found, &mut foreign);
    found && !foreign
}

fn expression_references_table(expression: &Expression, table_index: usize) -> bool {
    if matches!(
        expression,
        Expression::ColumnRef(column)
            if column.depth == 0 && column.binding.table_index == table_index
    ) {
        return true;
    }
    let mut found = false;
    ExpressionIterator::enumerate_children(expression, |child| {
        if !found {
            found = expression_references_table(child, table_index);
        }
    });
    found
}

fn expression_references_only_bindings(
    expression: &Expression,
    bindings: &HashSet<paro_planner::operator::ColumnBinding>,
) -> bool {
    match expression {
        Expression::ColumnRef(column) => column.depth == 0 && bindings.contains(&column.binding),
        _ => {
            let mut valid = true;
            ExpressionIterator::enumerate_children(expression, |child| {
                valid &= expression_references_only_bindings(child, bindings);
            });
            valid
        }
    }
}

fn correlated_join_condition(
    expression: Expression,
    delim_table_index: usize,
    outer_columns: &[Expression],
) -> Option<JoinCondition> {
    let Expression::Comparison(comparison) = expression else {
        return None;
    };
    correlated_condition_from_parts(
        *comparison.left,
        *comparison.right,
        comparison.comparison_type,
        delim_table_index,
        outer_columns,
    )
}

fn correlated_condition_from_parts(
    left: Expression,
    right: Expression,
    comparison: ComparisonType,
    delim_table_index: usize,
    outer_columns: &[Expression],
) -> Option<JoinCondition> {
    let left_is_delim = expression_references_only_table(&left, delim_table_index);
    let right_is_delim = expression_references_only_table(&right, delim_table_index);
    if left_is_delim == right_is_delim {
        return None;
    }

    let (delim_expression, base_expression, comparison_type) = if left_is_delim {
        (left, right, comparison)
    } else {
        (right, left, flip_comparison(comparison))
    };
    if expression_references_table(&base_expression, delim_table_index) {
        return None;
    }
    let outer_expression = delim_expression.replace_column_ref(&|column| {
        if column.depth != 0 || column.binding.table_index != delim_table_index {
            return None;
        }
        outer_columns.get(column.binding.column_index).cloned()
    });
    if expression_references_table(&outer_expression, delim_table_index) {
        return None;
    }

    Some(JoinCondition::new(
        outer_expression,
        base_expression,
        join_comparison_type(comparison_type),
    ))
}

fn flip_comparison(comparison: ComparisonType) -> ComparisonType {
    match comparison {
        ComparisonType::Equal => ComparisonType::Equal,
        ComparisonType::NotEqual => ComparisonType::NotEqual,
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        ComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

fn join_comparison_type(comparison: ComparisonType) -> JoinComparisonType {
    match comparison {
        ComparisonType::Equal => JoinComparisonType::Equal,
        ComparisonType::NotEqual => JoinComparisonType::NotEqual,
        ComparisonType::LessThan => JoinComparisonType::LessThan,
        ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
        ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
        ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
        ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
        ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
    }
}

fn join_to_expression_comparison(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

#[cfg(test)]
mod tests {
    use super::DelimJoinElimination;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    };
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, CrossProduct, DelimGet, ExpressionGet, Filter, Join,
        JoinComparisonType, JoinCondition, JoinType, LogicalOperator, Projection,
    };
    use paro_planner::plan::LogicalPlan;

    fn expression_get(table_index: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, 0),
                LogicalType::Integer,
            ))]],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )))
    }

    fn column(table_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, 0),
            LogicalType::Integer,
        ))
    }

    fn comparison(
        comparison_type: ComparisonType,
        left: Expression,
        right: Expression,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(comparison_type, left, right))
    }

    fn correlated_existence_join(project_delim_column: bool) -> ComparisonJoin {
        let outer = expression_get(0);
        let base = expression_get(1);
        let delim = LogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        )));
        let cross = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
            base, delim,
        ))));
        let correlated = comparison(ComparisonType::Equal, column(1), column(99));
        let local = comparison(
            ComparisonType::GreaterThan,
            column(1),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(5),
                LogicalType::Integer,
            )),
        );
        let filtered = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            cross,
            vec![correlated, local],
        )));
        let projected = LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            2,
            filtered,
            vec![if project_delim_column {
                column(99)
            } else {
                column(1)
            }],
        )));
        let mut join = ComparisonJoin::new(
            JoinType::Semi,
            outer,
            projected,
            vec![JoinCondition::new(
                column(0),
                column(2),
                JoinComparisonType::NotDistinctFrom,
            )],
        );
        join.duplicate_eliminated_columns = vec![column(0)];
        join
    }

    #[test]
    fn decorrelates_existence_and_preserves_base_local_filters() {
        let result = DelimJoinElimination::new().optimize_plan(LogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(correlated_existence_join(true))),
        ));
        let LogicalOperator::Join(Join::Comparison(join)) = result.operator else {
            panic!("expected direct existence join");
        };
        assert!(join.duplicate_eliminated_columns.is_empty());
        assert_eq!(join.conditions.len(), 1);
        let LogicalOperator::Filter(filter) = join.right.operator else {
            panic!("base-local predicate must remain on the direct build side");
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(matches!(
            filter.child.operator,
            LogicalOperator::ExpressionGet(_)
        ));
    }

    #[test]
    fn does_not_decorrelate_when_outer_join_does_not_bind_delim_output() {
        let result = DelimJoinElimination::new().optimize_plan(LogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(correlated_existence_join(false))),
        ));
        let LogicalOperator::Join(Join::Comparison(join)) = result.operator else {
            panic!("expected delimiter join to remain");
        };
        assert!(!join.duplicate_eliminated_columns.is_empty());
        assert!(matches!(
            join.right.operator,
            LogicalOperator::Projection(_)
        ));
    }

    #[test]
    fn removes_redundant_inner_join_with_delim_get() {
        let outer = expression_get(0);
        let base = expression_get(1);
        let delim_get = LogicalPlan::synthetic(LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        )));

        let redundant = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                base,
                delim_get,
                vec![JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(1, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(99, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::Equal,
                )],
            ),
        )));

        let mut root_join = ComparisonJoin::new(
            JoinType::Inner,
            outer,
            redundant,
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        root_join.duplicate_eliminated_columns = vec![Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer),
        )];

        let result = DelimJoinElimination::new().optimize_plan(LogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(root_join)),
        ));

        match result.operator {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(join.duplicate_eliminated_columns.is_empty());
                assert!(matches!(
                    join.right.operator,
                    LogicalOperator::Filter(_) | LogicalOperator::ExpressionGet(_)
                ));
                assert!(!matches!(join.right.operator, LogicalOperator::Join(_)));
            }
            _ => panic!("expected comparison join"),
        }
    }
}
