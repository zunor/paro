// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Eliminate redundant joins against `DelimGet`.

use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, Expression, OperatorExpression, OperatorType};
use paro_planner::operator::{Filter, Join, JoinType, LogicalOperator};
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

        let LogicalOperator::Join(Join::Comparison(mut join)) = op else {
            unreachable!("delim join rewrite must preserve comparison join root");
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

#[cfg(test)]
mod tests {
    use super::DelimJoinElimination;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, DelimGet, ExpressionGet, Join, JoinComparisonType,
        JoinCondition, JoinType, LogicalOperator,
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
