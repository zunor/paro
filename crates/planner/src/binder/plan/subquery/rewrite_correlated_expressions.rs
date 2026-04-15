//! Rewrites correlated column references to plain column refs against the duplicated outer scan.

use std::collections::HashMap;
use std::sync::Arc;

use crate::binder::plan::subquery::copy_subquery_top_level_plan;
use crate::binder::CorrelatedColumnInfo;
use crate::expression::{Expression, ExpressionIterator};
use crate::operator::{ColumnBinding, LogicalOperator};
use crate::plan::{LogicalPlan, PlannedStatement};

pub type CorrelatedColumnMap = HashMap<ColumnBinding, usize>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorrelatedLookupMode {
    ExactCurrentLayer,
    AnyOuterLayer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewriteRecursionMode {
    #[cfg(test)]
    Shallow,
    Recursive,
}

pub struct RewriteCorrelatedExpressions {
    base_binding: ColumnBinding,
    correlated_map: CorrelatedColumnMap,
    lateral_depth: usize,
    depth_shift: usize,
    recursion_mode: RewriteRecursionMode,
}

impl RewriteCorrelatedExpressions {
    #[cfg(test)]
    pub fn new_shallow(
        base_binding: ColumnBinding,
        correlated_map: CorrelatedColumnMap,
        lateral_depth: usize,
    ) -> Self {
        Self::new_with_mode(
            base_binding,
            correlated_map,
            lateral_depth,
            1,
            RewriteRecursionMode::Shallow,
        )
    }

    pub fn new_recursive(
        base_binding: ColumnBinding,
        correlated_map: CorrelatedColumnMap,
        lateral_depth: usize,
    ) -> Self {
        Self::new_with_mode(
            base_binding,
            correlated_map,
            lateral_depth,
            1,
            RewriteRecursionMode::Recursive,
        )
    }

    fn new_recursive_with_shift(
        base_binding: ColumnBinding,
        correlated_map: CorrelatedColumnMap,
        lateral_depth: usize,
        depth_shift: usize,
    ) -> Self {
        Self::new_with_mode(
            base_binding,
            correlated_map,
            lateral_depth,
            depth_shift.max(1),
            RewriteRecursionMode::Recursive,
        )
    }

    fn new_with_mode(
        base_binding: ColumnBinding,
        correlated_map: CorrelatedColumnMap,
        lateral_depth: usize,
        depth_shift: usize,
        recursion_mode: RewriteRecursionMode,
    ) -> Self {
        Self {
            base_binding,
            correlated_map,
            lateral_depth,
            depth_shift,
            recursion_mode,
        }
    }

    fn lookup_correlated_index(
        &self,
        binding: ColumnBinding,
        depth: usize,
        mode: CorrelatedLookupMode,
    ) -> Option<usize> {
        if depth <= self.lateral_depth {
            return None;
        }
        match mode {
            CorrelatedLookupMode::ExactCurrentLayer if depth != self.lateral_depth + 1 => {
                return None;
            }
            CorrelatedLookupMode::AnyOuterLayer => {}
            CorrelatedLookupMode::ExactCurrentLayer => {}
        }
        self.correlated_map.get(&binding).copied()
    }

    fn recursively_rewrites_nested_subqueries(&self) -> bool {
        matches!(self.recursion_mode, RewriteRecursionMode::Recursive)
    }

    pub fn rewrite_expression(&self, expr: Expression) -> Expression {
        let mut expr = expr;
        self.rewrite_expression_in_place(&mut expr);
        expr
    }

    fn rewrite_expression_in_place(&self, expr: &mut Expression) {
        match expr {
            Expression::ColumnRef(col_ref) => {
                let lookup_mode = if self.recursively_rewrites_nested_subqueries() {
                    CorrelatedLookupMode::AnyOuterLayer
                } else {
                    CorrelatedLookupMode::ExactCurrentLayer
                };
                if let Some(new_idx) =
                    self.lookup_correlated_index(col_ref.binding, col_ref.depth, lookup_mode)
                {
                    col_ref.binding = ColumnBinding::new(
                        self.base_binding.table_index,
                        self.base_binding.column_index + new_idx,
                    );
                    col_ref.depth = if self.recursively_rewrites_nested_subqueries() {
                        col_ref.depth.saturating_sub(self.depth_shift)
                    } else {
                        0
                    };
                }
            }
            Expression::Subquery(subquery) => {
                if self.recursively_rewrites_nested_subqueries()
                    && !subquery.correlated_columns.is_empty()
                {
                    let matched_depth_shift = subquery
                        .correlated_columns
                        .iter()
                        .filter_map(|corr| {
                            let binding = ColumnBinding::new(corr.table_index, corr.column_index);
                            self.correlated_map
                                .contains_key(&binding)
                                .then(|| corr.depth.saturating_sub(1))
                        })
                        .max()
                        .unwrap_or(1)
                        .max(1);
                    for corr in &mut subquery.correlated_columns {
                        let original_depth = corr.depth;
                        let binding = ColumnBinding::new(corr.table_index, corr.column_index);
                        if let Some(new_idx) = self.lookup_correlated_index(
                            binding,
                            corr.depth,
                            CorrelatedLookupMode::AnyOuterLayer,
                        ) {
                            corr.table_index = self.base_binding.table_index;
                            corr.column_index = self.base_binding.column_index + new_idx;
                            corr.depth = original_depth.saturating_sub(matched_depth_shift).max(1);
                        } else {
                            corr.depth = original_depth.saturating_sub(1).max(1);
                        }
                    }
                    self.rewrite_nested_subquery_plan(subquery, matched_depth_shift);
                }
            }
            Expression::Constant(_) | Expression::Reference(_) => {}
            _ => {
                ExpressionIterator::enumerate_children_mut(expr, |child| {
                    self.rewrite_expression_in_place(child);
                });
            }
        }
    }

    fn rewrite_nested_subquery_plan(
        &self,
        subquery: &mut crate::expression::SubqueryExpression,
        depth_shift: usize,
    ) {
        let copied = copy_subquery_top_level_plan(
            subquery.subquery.as_ref(),
            subquery.bind_snapshot.as_ref(),
        );
        let planned_rewriter = Self::new_recursive_with_shift(
            self.base_binding,
            self.correlated_map.clone(),
            self.lateral_depth + 1,
            depth_shift,
        );
        subquery.subquery = Arc::new(PlannedStatement {
            types: copied.types,
            names: copied.names,
            plan: planned_rewriter.rewrite_logical_plan(copied.plan),
        });
    }

    pub fn rewrite_logical_plan(&self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        LogicalPlan {
            id,
            stats,
            operator: self.rewrite_operator(operator),
        }
    }

    pub fn rewrite_operator(&self, op: LogicalOperator) -> LogicalOperator {
        match op {
            LogicalOperator::Filter(mut filter) => {
                filter.expressions = filter
                    .expressions
                    .into_iter()
                    .map(|e| self.rewrite_expression(e))
                    .collect();
                filter.child = Box::new(self.rewrite_logical_plan(*filter.child));
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Projection(mut proj) => {
                proj.expressions = proj
                    .expressions
                    .into_iter()
                    .map(|e| self.rewrite_expression(e))
                    .collect();
                proj.child = Box::new(self.rewrite_logical_plan(*proj.child));
                LogicalOperator::Projection(proj)
            }
            LogicalOperator::Aggregate(mut agg) => {
                agg.groups = agg
                    .groups
                    .into_iter()
                    .map(|e| self.rewrite_expression(e))
                    .collect();
                agg.aggregates = agg
                    .aggregates
                    .into_iter()
                    .map(|e| self.rewrite_expression(e))
                    .collect();
                agg.recompute_returned_types();
                agg.child = Box::new(self.rewrite_logical_plan(*agg.child));
                LogicalOperator::Aggregate(agg)
            }
            LogicalOperator::Order(mut order) => {
                order.orders = order
                    .orders
                    .into_iter()
                    .map(|mut o| {
                        o.expression = self.rewrite_expression(o.expression);
                        o
                    })
                    .collect();
                order.child = Box::new(self.rewrite_logical_plan(*order.child));
                LogicalOperator::Order(order)
            }
            LogicalOperator::Limit(mut limit) => {
                limit.child = Box::new(self.rewrite_logical_plan(*limit.child));
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::TopN(mut topn) => {
                topn.orders = topn
                    .orders
                    .into_iter()
                    .map(|mut o| {
                        o.expression = self.rewrite_expression(o.expression);
                        o
                    })
                    .collect();
                topn.child = Box::new(self.rewrite_logical_plan(*topn.child));
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Explain(mut explain) => {
                explain.child = Box::new(self.rewrite_logical_plan(*explain.child));
                LogicalOperator::Explain(explain)
            }
            LogicalOperator::EmptyResult(mut empty) => {
                empty.child = Box::new(self.rewrite_logical_plan(*empty.child));
                LogicalOperator::EmptyResult(empty)
            }
            LogicalOperator::Join(join) => {
                use crate::operator::Join;
                match join {
                    Join::Comparison(mut comp) => {
                        comp.conditions = comp
                            .conditions
                            .into_iter()
                            .map(|mut c| {
                                c.left = self.rewrite_expression(c.left);
                                c.right = self.rewrite_expression(c.right);
                                c
                            })
                            .collect();
                        comp.left = Box::new(self.rewrite_logical_plan(*comp.left));
                        comp.right = Box::new(self.rewrite_logical_plan(*comp.right));
                        LogicalOperator::Join(Join::Comparison(comp))
                    }
                    Join::Any(mut any) => {
                        any.condition = self.rewrite_expression(any.condition);
                        any.left = Box::new(self.rewrite_logical_plan(*any.left));
                        any.right = Box::new(self.rewrite_logical_plan(*any.right));
                        LogicalOperator::Join(Join::Any(any))
                    }
                    Join::Cross(mut cross) => {
                        cross.left = Box::new(self.rewrite_logical_plan(*cross.left));
                        cross.right = Box::new(self.rewrite_logical_plan(*cross.right));
                        LogicalOperator::Join(Join::Cross(cross))
                    }
                }
            }
            LogicalOperator::DependentJoin(mut dep) => {
                if let Some(cond) = dep.join_condition_mut() {
                    self.rewrite_expression_in_place(cond);
                }
                if let Some(payload) = dep.any_all_payload_mut() {
                    for expr in &mut payload.expression_children {
                        self.rewrite_expression_in_place(expr);
                    }
                }
                dep.left = Box::new(self.rewrite_logical_plan(*dep.left));
                let right_rewriter = Self {
                    base_binding: self.base_binding,
                    correlated_map: self.correlated_map.clone(),
                    lateral_depth: self.lateral_depth + 1,
                    depth_shift: self.depth_shift,
                    recursion_mode: self.recursion_mode,
                };
                dep.right = Box::new(right_rewriter.rewrite_logical_plan(*dep.right));
                LogicalOperator::DependentJoin(dep)
            }
            LogicalOperator::CopyTo(mut copy) => {
                copy.child = Box::new(self.rewrite_logical_plan(*copy.child));
                LogicalOperator::CopyTo(copy)
            }
            LogicalOperator::SetOperation(mut setop) => {
                setop.left = Box::new(self.rewrite_logical_plan(*setop.left));
                setop.right = Box::new(self.rewrite_logical_plan(*setop.right));
                LogicalOperator::SetOperation(setop)
            }
            LogicalOperator::Distinct(mut distinct) => {
                distinct.child = Box::new(self.rewrite_logical_plan(*distinct.child));
                LogicalOperator::Distinct(distinct)
            }
            LogicalOperator::Window(mut window) => {
                window.child = Box::new(self.rewrite_logical_plan(*window.child));
                LogicalOperator::Window(window)
            }
            LogicalOperator::MaterializedCTE(mut cte) => {
                cte.cte_query = Box::new(self.rewrite_logical_plan(*cte.cte_query));
                cte.child = Box::new(self.rewrite_logical_plan(*cte.child));
                LogicalOperator::MaterializedCTE(cte)
            }
            LogicalOperator::RecursiveCTE(mut cte) => {
                cte.anchor = Box::new(self.rewrite_logical_plan(*cte.anchor));
                cte.recursive = Box::new(self.rewrite_logical_plan(*cte.recursive));
                LogicalOperator::RecursiveCTE(cte)
            }
            LogicalOperator::Insert(mut insert) => {
                insert.child = Box::new(self.rewrite_logical_plan(*insert.child));
                LogicalOperator::Insert(insert)
            }
            LogicalOperator::Delete(mut delete) => {
                delete.child = Box::new(self.rewrite_logical_plan(*delete.child));
                LogicalOperator::Delete(delete)
            }
            LogicalOperator::Update(mut update) => {
                update.child = Box::new(self.rewrite_logical_plan(*update.child));
                LogicalOperator::Update(update)
            }
            LogicalOperator::GraphExpand(mut ge) => {
                ge.child = Box::new(self.rewrite_logical_plan(*ge.child));
                LogicalOperator::GraphExpand(ge)
            }
            LogicalOperator::Get(_)
            | LogicalOperator::DummyScan
            | LogicalOperator::ExpressionGet(_)
            | LogicalOperator::DelimGet(_)
            | LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::TableFunctionGet(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphScan(_) => op,
        }
    }
}

pub fn build_correlated_column_map(
    correlated_columns: &[CorrelatedColumnInfo],
) -> CorrelatedColumnMap {
    let mut map = HashMap::new();
    for (idx, corr) in correlated_columns.iter().enumerate() {
        let binding = ColumnBinding::new(corr.table_index, corr.column_index);
        map.insert(binding, idx);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{
        ColumnRefExpression, ComparisonType, Expression, SubqueryExpression, SubqueryPlanningState,
        SubqueryType,
    };
    use crate::operator::{DependentJoin, ExpressionGet, LogicalOperator};
    use crate::plan::LogicalPlan;
    use crate::{
        binder::context::BindContext, binder::CorrelatedColumnInfo, plan::PlannedStatement,
    };
    use paro_common::types::LogicalType;
    use std::sync::Arc;

    fn correlated_columns() -> Vec<CorrelatedColumnInfo> {
        vec![CorrelatedColumnInfo {
            table_index: 10,
            column_index: 0,
            return_type: LogicalType::Integer,
            name: "corr".to_string(),
            depth: 2,
        }]
    }

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            Vec::<Vec<Expression>>::new(),
            vec!["c0".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    #[test]
    fn rewrite_only_rewrites_matching_lateral_depth() {
        let mut map = CorrelatedColumnMap::new();
        map.insert(ColumnBinding::new(10, 0), 0);
        let rewriter = RewriteCorrelatedExpressions::new_shallow(ColumnBinding::new(99, 0), map, 1);

        let matched = Expression::ColumnRef(ColumnRefExpression::with_depth(
            ColumnBinding::new(10, 0),
            LogicalType::Integer,
            2,
        ));
        let deeper = Expression::ColumnRef(ColumnRefExpression::with_depth(
            ColumnBinding::new(10, 0),
            LogicalType::Integer,
            3,
        ));

        match rewriter.rewrite_expression(matched) {
            Expression::ColumnRef(col_ref) => {
                assert_eq!(col_ref.binding, ColumnBinding::new(99, 0));
                assert_eq!(col_ref.depth, 0);
            }
            other => panic!("expected rewritten column ref, got {other:?}"),
        }
        match rewriter.rewrite_expression(deeper) {
            Expression::ColumnRef(col_ref) => {
                assert_eq!(col_ref.binding, ColumnBinding::new(10, 0));
                assert_eq!(col_ref.depth, 3);
            }
            other => panic!("expected untouched column ref, got {other:?}"),
        }
    }

    #[test]
    fn recursive_rewrite_decrements_nested_subquery_depth() {
        let mut map = CorrelatedColumnMap::new();
        map.insert(ColumnBinding::new(10, 0), 0);
        let rewriter =
            RewriteCorrelatedExpressions::new_recursive(ColumnBinding::new(99, 0), map, 1);
        let subquery = SubqueryExpression {
            subquery_type: SubqueryType::Scalar,
            subquery: Arc::new(PlannedStatement {
                types: vec![LogicalType::Integer],
                names: vec!["c0".to_string()],
                plan: LogicalPlan::new(&BindContext::new(), expression_get(20)),
            }),
            children: vec![],
            child_types: vec![],
            child_targets: vec![],
            comparison_type: ComparisonType::Equal,
            return_type: LogicalType::Integer,
            correlated_columns: correlated_columns(),
            bind_snapshot: BindContext::new().snapshot(),
            planning_state: SubqueryPlanningState::Unplanned,
        };

        match rewriter.rewrite_expression(Expression::Subquery(subquery)) {
            Expression::Subquery(rewritten) => {
                assert_eq!(rewritten.correlated_columns[0].table_index, 99);
                assert_eq!(rewritten.correlated_columns[0].column_index, 0);
                assert_eq!(rewritten.correlated_columns[0].depth, 1);
            }
            other => panic!("expected rewritten subquery, got {other:?}"),
        }
    }

    #[test]
    fn recursive_rewrite_keeps_current_layer_subquery_depth_at_one() {
        let mut map = CorrelatedColumnMap::new();
        map.insert(ColumnBinding::new(10, 0), 0);
        let rewriter =
            RewriteCorrelatedExpressions::new_recursive(ColumnBinding::new(99, 0), map, 0);
        let subquery = SubqueryExpression {
            subquery_type: SubqueryType::Scalar,
            subquery: Arc::new(PlannedStatement {
                types: vec![LogicalType::Integer],
                names: vec!["c0".to_string()],
                plan: LogicalPlan::new(&BindContext::new(), expression_get(20)),
            }),
            children: vec![],
            child_types: vec![],
            child_targets: vec![],
            comparison_type: ComparisonType::Equal,
            return_type: LogicalType::Integer,
            correlated_columns: vec![CorrelatedColumnInfo {
                table_index: 10,
                column_index: 0,
                return_type: LogicalType::Integer,
                name: "corr".to_string(),
                depth: 1,
            }],
            bind_snapshot: BindContext::new().snapshot(),
            planning_state: SubqueryPlanningState::Unplanned,
        };

        match rewriter.rewrite_expression(Expression::Subquery(subquery)) {
            Expression::Subquery(rewritten) => {
                assert_eq!(rewritten.correlated_columns[0].table_index, 99);
                assert_eq!(rewritten.correlated_columns[0].column_index, 0);
                assert_eq!(rewritten.correlated_columns[0].depth, 1);
            }
            other => panic!("expected rewritten subquery, got {other:?}"),
        }
    }

    #[test]
    fn recursive_rewrite_decrements_nested_subquery_depth_even_without_binding_rebase() {
        let mut map = CorrelatedColumnMap::new();
        map.insert(ColumnBinding::new(10, 0), 0);
        let rewriter =
            RewriteCorrelatedExpressions::new_recursive(ColumnBinding::new(99, 0), map, 1);
        let subquery = SubqueryExpression {
            subquery_type: SubqueryType::Scalar,
            subquery: Arc::new(PlannedStatement {
                types: vec![LogicalType::Integer],
                names: vec!["c0".to_string()],
                plan: LogicalPlan::new(&BindContext::new(), expression_get(20)),
            }),
            children: vec![],
            child_types: vec![],
            child_targets: vec![],
            comparison_type: ComparisonType::Equal,
            return_type: LogicalType::Integer,
            correlated_columns: vec![CorrelatedColumnInfo {
                table_index: 77,
                column_index: 0,
                return_type: LogicalType::Integer,
                name: "corr".to_string(),
                depth: 2,
            }],
            bind_snapshot: BindContext::new().snapshot(),
            planning_state: SubqueryPlanningState::Unplanned,
        };

        match rewriter.rewrite_expression(Expression::Subquery(subquery)) {
            Expression::Subquery(rewritten) => {
                assert_eq!(rewritten.correlated_columns[0].table_index, 77);
                assert_eq!(rewritten.correlated_columns[0].column_index, 0);
                assert_eq!(rewritten.correlated_columns[0].depth, 1);
            }
            other => panic!("expected rewritten subquery, got {other:?}"),
        }
    }

    #[test]
    fn recursive_rewrite_increments_lateral_depth_for_dependent_join_rhs() {
        let mut map = CorrelatedColumnMap::new();
        map.insert(ColumnBinding::new(10, 0), 0);
        let rewriter =
            RewriteCorrelatedExpressions::new_recursive(ColumnBinding::new(99, 0), map, 0);
        let ctx = BindContext::new();
        let dep = DependentJoin::scalar(
            LogicalPlan::new(&ctx, expression_get(1)),
            LogicalPlan::new(
                &ctx,
                LogicalOperator::Projection(crate::operator::Projection::new(
                    2,
                    LogicalPlan::new(&ctx, expression_get(3)),
                    vec![Expression::ColumnRef(ColumnRefExpression::with_depth(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                        2,
                    ))],
                )),
            ),
            vec![CorrelatedColumnInfo {
                table_index: 10,
                column_index: 0,
                return_type: LogicalType::Integer,
                name: "corr".to_string(),
                depth: 1,
            }],
        );

        match rewriter.rewrite_operator(LogicalOperator::DependentJoin(dep)) {
            LogicalOperator::DependentJoin(dep) => match &dep.right.operator {
                LogicalOperator::Projection(proj) => match &proj.expressions[0] {
                    Expression::ColumnRef(col_ref) => {
                        assert_eq!(col_ref.binding, ColumnBinding::new(99, 0));
                        assert_eq!(col_ref.depth, 1);
                    }
                    other => panic!("expected rewritten column ref, got {other:?}"),
                },
                other => panic!("expected projection rhs, got {other:?}"),
            },
            other => panic!("expected dependent join, got {other:?}"),
        }
    }
}
