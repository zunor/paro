use crate::binder::context::BindSnapshot;
use crate::binder::deep_copy::{
    deep_copy_operator_shallow_subqueries, deep_copy_plan_shallow_subqueries,
};
use crate::operator::LogicalOperator;
use crate::plan::PlannedStatement;

pub fn copy_subquery_top_level(
    op: &LogicalOperator,
    bind_snapshot: &BindSnapshot,
) -> LogicalOperator {
    deep_copy_operator_shallow_subqueries(op, bind_snapshot.shared().as_ref())
}

pub fn copy_subquery_top_level_plan(
    stmt: &PlannedStatement,
    bind_snapshot: &BindSnapshot,
) -> PlannedStatement {
    PlannedStatement {
        types: stmt.types.clone(),
        names: stmt.names.clone(),
        plan: deep_copy_plan_shallow_subqueries(&stmt.plan, bind_snapshot.shared().as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::types::LogicalType;

    use super::*;
    use crate::binder::context::BindContext;
    use crate::expression::{
        ComparisonType, Expression, SubqueryExpression, SubqueryPlanningState, SubqueryType,
    };
    use crate::operator::{ExpressionGet, LogicalOperator, Projection};
    use crate::plan::LogicalPlan;

    fn expression_get(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    #[test]
    fn top_level_copy_keeps_nested_subquery_payload_shallow() {
        let nested_stmt = Arc::new(PlannedStatement {
            types: vec![LogicalType::Integer],
            names: vec!["v".to_string()],
            plan: LogicalPlan::new(&BindContext::new(), expression_get(99)),
        });
        let nested_bind_snapshot = BindContext::new().snapshot();
        let root_ctx = BindContext::new();
        let op = LogicalOperator::Projection(Projection::new(
            11,
            LogicalPlan::new(&root_ctx, expression_get(7)),
            vec![Expression::Subquery(SubqueryExpression {
                subquery_type: SubqueryType::Scalar,
                subquery: Arc::clone(&nested_stmt),
                children: vec![],
                child_types: vec![],
                child_targets: vec![],
                comparison_type: ComparisonType::Equal,
                return_type: LogicalType::Integer,
                correlated_columns: vec![],
                bind_snapshot: Arc::clone(&nested_bind_snapshot),
                planning_state: SubqueryPlanningState::Unplanned,
            })],
        ));

        let copied = copy_subquery_top_level(&op, nested_bind_snapshot.as_ref());
        let LogicalOperator::Projection(projection) = copied else {
            panic!("expected projection");
        };
        let Expression::Subquery(copied_subquery) = &projection.expressions[0] else {
            panic!("expected copied subquery expression");
        };

        assert!(Arc::ptr_eq(&copied_subquery.subquery, &nested_stmt));
        assert!(Arc::ptr_eq(
            &copied_subquery.bind_snapshot,
            &nested_bind_snapshot
        ));
        assert_eq!(
            copied_subquery.planning_state,
            SubqueryPlanningState::Unplanned
        );

        let LogicalOperator::ExpressionGet(nested_get) = &copied_subquery.subquery.plan.operator
        else {
            panic!("expected nested expression get");
        };
        assert_eq!(nested_get.table_index, 99);

        let LogicalOperator::ExpressionGet(top_get) = &projection.child.operator else {
            panic!("expected top-level expression get");
        };
        assert_ne!(top_get.table_index, 7);
    }
}
