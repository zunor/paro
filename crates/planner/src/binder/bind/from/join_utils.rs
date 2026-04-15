//! JOIN Planning Utilities
//!
//! Helper functions for extracting and classifying JOIN conditions.

use std::collections::HashSet;

use crate::binder::context::BindContext;
use crate::binder::ir::JoinType as BinderJoinType;
use crate::expression::*;
use crate::operator::{
    AnyJoin, ComparisonJoin, Filter, Join, JoinComparisonType, JoinCondition, JoinSide, JoinType,
    LogicalOperator,
};
use crate::plan::LogicalPlan;
use paro_common::error::Result;

pub fn convert_join_type(binder_type: BinderJoinType) -> JoinType {
    match binder_type {
        BinderJoinType::Inner => JoinType::Inner,
        BinderJoinType::Left => JoinType::Left,
        BinderJoinType::Right => JoinType::Right,
        BinderJoinType::Full => JoinType::Outer,
        BinderJoinType::Cross => JoinType::Inner,
        BinderJoinType::LeftSemi => JoinType::Semi,
        BinderJoinType::RightSemi => JoinType::RightSemi,
        BinderJoinType::LeftAnti => JoinType::Anti,
        BinderJoinType::RightAnti => JoinType::RightAnti,
    }
}

pub fn collect_table_bindings(op: &LogicalOperator) -> HashSet<usize> {
    let mut bindings = HashSet::new();
    collect_table_bindings_recursive(op, &mut bindings);
    bindings
}

fn collect_table_bindings_recursive(op: &LogicalOperator, bindings: &mut HashSet<usize>) {
    match op {
        LogicalOperator::Get(get) => {
            bindings.insert(get.table_index);
        }
        LogicalOperator::Filter(filter) => {
            collect_table_bindings_recursive(&filter.child.operator, bindings);
        }
        LogicalOperator::Projection(proj) => {
            collect_table_bindings_recursive(&proj.child.operator, bindings);
        }
        LogicalOperator::Limit(limit) => {
            collect_table_bindings_recursive(&limit.child.operator, bindings);
        }
        LogicalOperator::Order(order) => {
            collect_table_bindings_recursive(&order.child.operator, bindings);
        }
        LogicalOperator::TopN(topn) => {
            collect_table_bindings_recursive(&topn.child.operator, bindings);
        }
        LogicalOperator::Aggregate(agg) => {
            collect_table_bindings_recursive(&agg.child.operator, bindings);
        }
        LogicalOperator::Insert(insert) => {
            collect_table_bindings_recursive(&insert.child.operator, bindings);
        }
        LogicalOperator::Delete(delete) => {
            collect_table_bindings_recursive(&delete.child.operator, bindings);
        }
        LogicalOperator::Update(update) => {
            collect_table_bindings_recursive(&update.child.operator, bindings);
        }
        LogicalOperator::CopyTo(copy) => {
            collect_table_bindings_recursive(&copy.child.operator, bindings);
        }
        LogicalOperator::Join(join) => {
            collect_table_bindings_recursive(&join.left().operator, bindings);
            collect_table_bindings_recursive(&join.right().operator, bindings);
        }
        LogicalOperator::SetOperation(setop) => {
            collect_table_bindings_recursive(&setop.left().operator, bindings);
            collect_table_bindings_recursive(&setop.right().operator, bindings);
        }
        LogicalOperator::ExpressionGet(expr_get) => {
            bindings.insert(expr_get.table_index);
        }
        LogicalOperator::DelimGet(delim_get) => {
            bindings.insert(delim_get.table_index);
        }
        LogicalOperator::Distinct(distinct) => {
            collect_table_bindings_recursive(&distinct.child.operator, bindings);
        }
        LogicalOperator::Window(window) => {
            collect_table_bindings_recursive(&window.child.operator, bindings);
        }
        LogicalOperator::Explain(explain) => {
            collect_table_bindings_recursive(&explain.child.operator, bindings);
        }
        LogicalOperator::EmptyResult(empty) => {
            collect_table_bindings_recursive(&empty.child.operator, bindings);
        }
        LogicalOperator::MaterializedCTE(cte) => {
            collect_table_bindings_recursive(&cte.cte_query.operator, bindings);
            collect_table_bindings_recursive(&cte.child.operator, bindings);
        }
        LogicalOperator::RecursiveCTE(cte) => {
            collect_table_bindings_recursive(&cte.anchor.operator, bindings);
            collect_table_bindings_recursive(&cte.recursive.operator, bindings);
        }
        LogicalOperator::CTERef(cte_ref) => {
            bindings.insert(cte_ref.table_index);
        }
        LogicalOperator::TableFunctionGet(tf) => {
            bindings.insert(tf.table_index);
        }
        LogicalOperator::SearchScan(search) => {
            bindings.insert(search.get.table_index);
            bindings.insert(search.projection_table_index);
        }
        LogicalOperator::FullTextFilterScan(scan) => {
            bindings.insert(scan.get.table_index);
        }
        LogicalOperator::DependentJoin(dep) => {
            collect_table_bindings_recursive(&dep.left.operator, bindings);
            collect_table_bindings_recursive(&dep.right.operator, bindings);
        }
        LogicalOperator::Alter(_)
        | LogicalOperator::CreateTable(_)
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
        | LogicalOperator::DummyScan => {}
        LogicalOperator::GraphExpand(ge) => {
            collect_table_bindings_recursive(&ge.child.operator, bindings);
        }
    }
}

pub fn split_conjunction(expr: Expression) -> Vec<Expression> {
    match expr {
        Expression::Conjunction(conj) if conj.conjunction_type == ConjunctionType::And => {
            let mut result = Vec::new();
            for child in conj.children {
                result.extend(split_conjunction(child));
            }
            result
        }
        _ => vec![expr],
    }
}

pub fn extract_join_condition(
    expr: Expression,
    left_bindings: &HashSet<usize>,
    right_bindings: &HashSet<usize>,
    conditions: &mut Vec<JoinCondition>,
    arbitrary_expressions: &mut Vec<Expression>,
) {
    if let Expression::Comparison(comp) = &expr {
        let left_side = get_expression_side(&comp.left, left_bindings, right_bindings);
        let right_side = get_expression_side(&comp.right, left_bindings, right_bindings);

        if left_side != JoinSide::Both && right_side != JoinSide::Both {
            if let Some(comparison_type) = convert_comparison_type(comp.comparison_type) {
                let (left_expr, right_expr, final_comparison) =
                    if left_side == JoinSide::Right || right_side == JoinSide::Left {
                        (
                            (*comp.right).clone(),
                            (*comp.left).clone(),
                            comparison_type.flip(),
                        )
                    } else {
                        ((*comp.left).clone(), (*comp.right).clone(), comparison_type)
                    };

                conditions.push(JoinCondition::new(left_expr, right_expr, final_comparison));
                return;
            }
        }
    }

    arbitrary_expressions.push(expr);
}

pub fn get_expression_side(
    expr: &Expression,
    left_bindings: &HashSet<usize>,
    right_bindings: &HashSet<usize>,
) -> JoinSide {
    match expr {
        Expression::ColumnRef(col) => {
            JoinSide::get_side(col.binding.table_index, left_bindings, right_bindings)
        }
        Expression::Constant(_) => JoinSide::None,
        Expression::Function(func) => {
            let mut side = JoinSide::None;
            for child in &func.children {
                side = JoinSide::combine(
                    side,
                    get_expression_side(child, left_bindings, right_bindings),
                );
            }
            side
        }
        Expression::Cast(cast) => get_expression_side(&cast.child, left_bindings, right_bindings),
        Expression::Conjunction(conj) => {
            let mut side = JoinSide::None;
            for child in &conj.children {
                side = JoinSide::combine(
                    side,
                    get_expression_side(child, left_bindings, right_bindings),
                );
            }
            side
        }
        Expression::Comparison(comp) => {
            let left_side = get_expression_side(&comp.left, left_bindings, right_bindings);
            let right_side = get_expression_side(&comp.right, left_bindings, right_bindings);
            JoinSide::combine(left_side, right_side)
        }
        Expression::Operator(op) => {
            let mut side = JoinSide::None;
            for child in &op.children {
                side = JoinSide::combine(
                    side,
                    get_expression_side(child, left_bindings, right_bindings),
                );
            }
            side
        }
        Expression::Case(case) => {
            let check_side = get_expression_side(&case.check, left_bindings, right_bindings);
            let true_side =
                get_expression_side(&case.result_if_true, left_bindings, right_bindings);
            let false_side =
                get_expression_side(&case.result_if_false, left_bindings, right_bindings);
            JoinSide::combine(JoinSide::combine(check_side, true_side), false_side)
        }
        Expression::Reference(_) => JoinSide::None,
        Expression::Aggregate(agg) => {
            let mut side = JoinSide::None;
            for child in &agg.children {
                side = JoinSide::combine(
                    side,
                    get_expression_side(child, left_bindings, right_bindings),
                );
            }
            if let Some(filter) = &agg.filter {
                side = JoinSide::combine(
                    side,
                    get_expression_side(filter, left_bindings, right_bindings),
                );
            }
            for order in &agg.order_bys {
                side = JoinSide::combine(
                    side,
                    get_expression_side(&order.expression, left_bindings, right_bindings),
                );
            }
            side
        }
        Expression::Subquery(_) => JoinSide::Both,
        Expression::Window(window) => {
            let mut side = JoinSide::None;
            for child in &window.children {
                side = JoinSide::combine(
                    side,
                    get_expression_side(child, left_bindings, right_bindings),
                );
            }
            for partition in &window.partitions {
                side = JoinSide::combine(
                    side,
                    get_expression_side(partition, left_bindings, right_bindings),
                );
            }
            for order in &window.orders {
                side = JoinSide::combine(
                    side,
                    get_expression_side(&order.expression, left_bindings, right_bindings),
                );
            }
            side
        }
    }
}

fn convert_comparison_type(comp_type: ComparisonType) -> Option<JoinComparisonType> {
    match comp_type {
        ComparisonType::Equal => Some(JoinComparisonType::Equal),
        ComparisonType::NotEqual => Some(JoinComparisonType::NotEqual),
        ComparisonType::LessThan => Some(JoinComparisonType::LessThan),
        ComparisonType::LessThanOrEqual => Some(JoinComparisonType::LessThanOrEqual),
        ComparisonType::GreaterThan => Some(JoinComparisonType::GreaterThan),
        ComparisonType::GreaterThanOrEqual => Some(JoinComparisonType::GreaterThanOrEqual),
        ComparisonType::NotDistinctFrom => Some(JoinComparisonType::NotDistinctFrom),
        ComparisonType::DistinctFrom => Some(JoinComparisonType::DistinctFrom),
    }
}

pub fn create_join_operator(
    bind_ctx: &BindContext,
    join_type: JoinType,
    left_child: LogicalOperator,
    right_child: LogicalOperator,
    conditions: Vec<JoinCondition>,
    arbitrary_expressions: Vec<Expression>,
) -> Result<LogicalOperator> {
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    if conditions.is_empty() {
        let condition = if arbitrary_expressions.is_empty() {
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })
        } else {
            combine_expressions_with_and(arbitrary_expressions)
        };

        let any_join = AnyJoin::new(
            join_type,
            LogicalPlan::new(bind_ctx, left_child),
            LogicalPlan::new(bind_ctx, right_child),
            condition,
        );
        return Ok(LogicalOperator::Join(Join::Any(Box::new(any_join))));
    }

    if arbitrary_expressions.is_empty() {
        let comp_join = ComparisonJoin::new(
            join_type,
            LogicalPlan::new(bind_ctx, left_child),
            LogicalPlan::new(bind_ctx, right_child),
            conditions,
        );
        return Ok(LogicalOperator::Join(Join::Comparison(comp_join)));
    }

    if join_type == JoinType::Inner {
        let comp_join = ComparisonJoin::new(
            join_type,
            LogicalPlan::new(bind_ctx, left_child),
            LogicalPlan::new(bind_ctx, right_child),
            conditions,
        );
        let join_op = LogicalOperator::Join(Join::Comparison(comp_join));

        let filter = Filter::new(LogicalPlan::new(bind_ctx, join_op), arbitrary_expressions);
        Ok(LogicalOperator::Filter(filter))
    } else {
        let mut all_expressions: Vec<Expression> = conditions
            .into_iter()
            .map(join_condition_to_expression)
            .collect();
        all_expressions.extend(arbitrary_expressions);

        let condition = combine_expressions_with_and(all_expressions);
        let any_join = AnyJoin::new(
            join_type,
            LogicalPlan::new(bind_ctx, left_child),
            LogicalPlan::new(bind_ctx, right_child),
            condition,
        );
        Ok(LogicalOperator::Join(Join::Any(Box::new(any_join))))
    }
}

fn join_condition_to_expression(cond: JoinCondition) -> Expression {
    let comp_type = match cond.comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
    };

    Expression::Comparison(ComparisonExpression::new(comp_type, cond.left, cond.right))
}

fn combine_expressions_with_and(mut expressions: Vec<Expression>) -> Expression {
    if expressions.len() == 1 {
        return expressions.pop().unwrap();
    }

    Expression::Conjunction(ConjunctionExpression {
        conjunction_type: ConjunctionType::And,
        children: expressions,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use paro_common::types::LogicalType;

    use super::extract_join_condition;
    use crate::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, Expression,
    };
    use crate::operator::{ColumnBinding, JoinComparisonType};

    fn col(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn extract_join_condition_keeps_left_and_right_operands_in_child_order() {
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            col(6, 0),
            col(7, 0),
        ));
        let left_bindings = HashSet::from([6]);
        let right_bindings = HashSet::from([7]);
        let mut conditions = Vec::new();
        let mut arbitrary_expressions = Vec::new();

        extract_join_condition(
            expr,
            &left_bindings,
            &right_bindings,
            &mut conditions,
            &mut arbitrary_expressions,
        );

        assert!(arbitrary_expressions.is_empty());
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].comparison, JoinComparisonType::Equal);

        match (&conditions[0].left, &conditions[0].right) {
            (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
                assert_eq!(left.binding, ColumnBinding::new(6, 0));
                assert_eq!(right.binding, ColumnBinding::new(7, 0));
            }
            other => panic!("expected column refs, got {:?}", other),
        }
    }
}
