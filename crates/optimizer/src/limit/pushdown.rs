// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Push constant `LIMIT` nodes below projections when the rewrite is cheap.

use paro_planner::expression::Expression;
use paro_planner::operator::{LogicalOperator, LogicalOperatorType};
use paro_planner::plan::LogicalPlan;

pub struct LimitPushdown;

impl LimitPushdown {
    pub fn new() -> Self {
        Self
    }

    #[cfg(test)]
    fn optimize(&mut self, plan: LogicalOperator) -> LogicalOperator {
        self.optimize_plan(LogicalPlan::synthetic(plan)).operator
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_recursive_plan(plan)
    }

    fn optimize_recursive_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_recursive_plan(child));
        if Self::can_optimize(&plan.operator) {
            self.apply_optimization(plan)
        } else {
            plan
        }
    }

    fn can_optimize(plan: &LogicalOperator) -> bool {
        let LogicalOperator::Limit(limit) = plan else {
            return false;
        };

        if limit.child.operator.op_type() != LogicalOperatorType::Projection {
            return false;
        }

        let Some(limit_expr) = &limit.limit else {
            return false;
        };
        let Some(limit_val) = Self::extract_constant_value(limit_expr) else {
            return false;
        };
        if limit_val >= 8192 {
            return false;
        }

        if let Some(offset_expr) = &limit.offset {
            if Self::extract_constant_value(offset_expr).is_none() {
                return false;
            }
        }

        true
    }

    fn extract_constant_value(expr: &Expression) -> Option<usize> {
        if let Expression::Constant(const_expr) = expr {
            match &const_expr.value {
                paro_common::runtime_value::Value::TinyInt(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::SmallInt(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::Integer(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::BigInt(v) => usize::try_from(*v).ok(),
                _ => None,
            }
        } else {
            None
        }
    }

    fn apply_optimization(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        let LogicalOperator::Limit(mut limit) = operator else {
            return LogicalPlan {
                id,
                stats,
                operator,
            };
        };

        let child_plan = *limit.child;
        let LogicalPlan {
            id: child_id,
            stats: child_stats,
            operator: child_operator,
        } = child_plan;
        let LogicalOperator::Projection(mut projection) = child_operator else {
            limit.child = Box::new(LogicalPlan {
                id: child_id,
                stats: child_stats,
                operator: child_operator,
            });
            return LogicalPlan {
                id,
                stats,
                operator: LogicalOperator::Limit(limit),
            };
        };

        let inner_child = *projection.child;
        limit.child = Box::new(inner_child);
        projection.child = Box::new(LogicalPlan::synthetic(LogicalOperator::Limit(limit)));

        LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Projection(projection),
        }
    }
}

impl Default for LimitPushdown {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ConstantExpression, Expression};
    use paro_planner::operator::{Get, Limit, Projection};

    fn create_test_get() -> LogicalOperator {
        LogicalOperator::Get(Get {
            table_index: 0,
            returned_types: vec![LogicalType::Integer, LogicalType::Varchar],
            names: vec!["id".to_string(), "name".to_string()],
            relation_name: None,
            relation_alias: None,
            column_ids: vec![0, 1],
            column_types: vec![LogicalType::Integer, LogicalType::Varchar],
            table: None,
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        })
    }

    fn create_constant_expr(value: i64) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::BigInt(value),
            return_type: LogicalType::BigInt,
        })
    }

    fn create_projection(child: LogicalOperator) -> LogicalOperator {
        LogicalOperator::Projection(Projection {
            table_index: 1,
            expressions: vec![
                Expression::Constant(ConstantExpression {
                    value: Value::Integer(0),
                    return_type: LogicalType::Integer,
                }),
                Expression::Constant(ConstantExpression {
                    value: Value::Integer(1),
                    return_type: LogicalType::Integer,
                }),
            ],
            output_names: vec!["id".to_string(), "name".to_string()],
            returned_types: vec![LogicalType::Integer, LogicalType::Varchar],
            child: Box::new(LogicalPlan::synthetic(child)),
        })
    }

    #[test]
    fn test_can_optimize_simple() {
        let get = create_test_get();
        let projection = create_projection(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(10)),
            None,
        ));

        assert!(LimitPushdown::can_optimize(&limit));
    }

    #[test]
    fn test_can_optimize_with_offset() {
        let get = create_test_get();
        let projection = create_projection(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(10)),
            Some(create_constant_expr(5)),
        ));

        assert!(LimitPushdown::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_large_limit() {
        let get = create_test_get();
        let projection = create_projection(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(10000)),
            None,
        ));

        assert!(!LimitPushdown::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_no_projection() {
        let get = create_test_get();
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(get),
            Some(create_constant_expr(10)),
            None,
        ));

        assert!(!LimitPushdown::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_negative_offset() {
        let get = create_test_get();
        let projection = create_projection(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(10)),
            Some(create_constant_expr(-1)),
        ));

        assert!(!LimitPushdown::can_optimize(&limit));
    }

    #[test]
    fn test_optimize_simple() {
        let mut optimizer = LimitPushdown::new();
        let get = create_test_get();
        let projection = create_projection(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(10)),
            None,
        ));

        let result = optimizer.optimize(limit);

        // Result should be PROJECTION → LIMIT → GET
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = result {
            assert_eq!(proj.child.operator.op_type(), LogicalOperatorType::Limit);
            if let LogicalOperator::Limit(limit) = &proj.child.operator {
                assert_eq!(limit.child.operator.op_type(), LogicalOperatorType::Get);
            } else {
                panic!("Expected Limit as child of Projection");
            }
        } else {
            panic!("Expected Projection operator");
        }
    }

    #[test]
    fn test_no_optimization_when_not_applicable() {
        let mut optimizer = LimitPushdown::new();
        let get = create_test_get();
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(get),
            Some(create_constant_expr(10)),
            None,
        ));

        let result = optimizer.optimize(limit);

        // Should remain as LIMIT → GET since there's no PROJECTION
        assert_eq!(result.op_type(), LogicalOperatorType::Limit);
        if let LogicalOperator::Limit(limit) = result {
            assert_eq!(limit.child.operator.op_type(), LogicalOperatorType::Get);
        } else {
            panic!("Expected Limit operator");
        }
    }
}
