// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Rewrite `ORDER BY ... LIMIT/OFFSET` into `TopN`.

use paro_planner::expression::Expression;
use paro_planner::operator::{LogicalOperator, LogicalOperatorType, Projection, TopN};
use paro_planner::plan::LogicalPlan;

pub struct TopNOptimizer;

impl TopNOptimizer {
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

        let Some(limit_expr) = &limit.limit else {
            return false;
        };
        if Self::extract_constant_value(limit_expr).is_none() {
            return false;
        }

        if let Some(offset_expr) = &limit.offset {
            if Self::extract_constant_value(offset_expr).is_none() {
                return false;
            }
        }

        let mut child = limit.child.as_ref();
        while child.operator.op_type() == LogicalOperatorType::Projection {
            if let LogicalOperator::Projection(proj) = &child.operator {
                if proj
                    .expressions
                    .iter()
                    .any(|expr| !expr.evaluation_properties().can_share_evaluation())
                {
                    return false;
                }
                child = proj.child.as_ref();
            } else {
                break;
            }
        }

        child.operator.op_type() == LogicalOperatorType::Order
    }

    fn extract_constant_value(expr: &Expression) -> Option<usize> {
        if let Expression::Constant(const_expr) = expr {
            match &const_expr.value {
                paro_common::runtime_value::Value::TinyInt(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::SmallInt(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::Integer(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::BigInt(v) => usize::try_from(*v).ok(),
                paro_common::runtime_value::Value::UTinyInt(v) => Some(*v as usize),
                paro_common::runtime_value::Value::USmallInt(v) => Some(*v as usize),
                paro_common::runtime_value::Value::UInteger(v) => Some(*v as usize),
                paro_common::runtime_value::Value::UBigInt(v) => usize::try_from(*v).ok(),
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
        let LogicalOperator::Limit(limit) = operator else {
            return LogicalPlan {
                id,
                stats,
                operator,
            };
        };

        let limit_val = Self::extract_constant_value(limit.limit.as_ref().unwrap()).unwrap_or(0);
        let offset_val = limit
            .offset
            .as_ref()
            .and_then(Self::extract_constant_value)
            .unwrap_or(0);

        let mut projections: Vec<(usize, Vec<Expression>, Vec<String>, usize, Option<String>)> =
            Vec::new();
        let mut child_lp = *limit.child;

        loop {
            let op = child_lp.operator;
            let LogicalOperator::Projection(proj) = op else {
                child_lp.operator = op;
                break;
            };
            let Projection {
                table_index,
                expressions,
                visible_names,
                visible_count,
                visible_qualifier,
                child,
                ..
            } = proj;
            let inner = *child;
            projections.push((
                table_index,
                expressions,
                visible_names,
                visible_count,
                visible_qualifier,
            ));
            child_lp = inner;
        }

        let op = child_lp.operator;
        let order = match op {
            LogicalOperator::Order(o) => o,
            other => {
                child_lp.operator = other;
                let mut result =
                    LogicalOperator::Order(paro_planner::operator::Order::new(child_lp, vec![]));
                while let Some((table_index, expressions, output_names, visible_count, qualifier)) =
                    projections.pop()
                {
                    let mut proj =
                        Projection::new(table_index, LogicalPlan::synthetic(result), expressions)
                            .with_visible_names(output_names);
                    proj.visible_count = visible_count;
                    if let Some(qualifier) = qualifier {
                        proj = proj.with_visible_qualifier(qualifier);
                    }
                    result = LogicalOperator::Projection(proj);
                }
                return LogicalPlan {
                    id,
                    stats,
                    operator: result,
                };
            }
        };

        let order_child = *order.child;
        let topn = TopN::new(order_child, order.orders, limit_val, offset_val)
            .with_hnsw_ef_hint(limit.hnsw_ef_hint);
        let mut result = LogicalOperator::TopN(topn);

        while let Some((table_index, expressions, output_names, visible_count, qualifier)) =
            projections.pop()
        {
            let mut proj =
                Projection::new(table_index, LogicalPlan::synthetic(result), expressions)
                    .with_visible_names(output_names);
            proj.visible_count = visible_count;
            if let Some(qualifier) = qualifier {
                proj = proj.with_visible_qualifier(qualifier);
            }
            result = LogicalOperator::Projection(proj);
        }

        LogicalPlan {
            id,
            stats,
            operator: result,
        }
    }
}

impl Default for TopNOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::binder::ir::OrderByNode;
    use paro_planner::expression::{ConstantExpression, Expression, FunctionExpression};
    use paro_planner::operator::{Get, Limit, Order, Projection};

    fn create_test_get() -> LogicalOperator {
        LogicalOperator::Get(Get {
            table_index: 0,
            returned_types: vec![LogicalType::Integer, LogicalType::Integer],
            names: vec!["a".to_string(), "b".to_string()],
            relation_name: None,
            relation_alias: None,
            column_sources: vec![
                paro_planner::operator::GetColumnSource::Stored { column_id: 0 },
                paro_planner::operator::GetColumnSource::Stored { column_id: 1 },
            ],
            column_types: vec![LogicalType::Integer, LogicalType::Integer],
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

    fn create_order_by(child: LogicalOperator) -> LogicalOperator {
        LogicalOperator::Order(Order::new(
            LogicalPlan::synthetic(child),
            vec![OrderByNode {
                expression: Expression::Constant(ConstantExpression {
                    value: Value::Integer(1),
                    return_type: LogicalType::Integer,
                }),
                ascending: true,
                nulls_first: false,
            }],
        ))
    }

    fn create_projection(child: LogicalOperator) -> LogicalOperator {
        LogicalOperator::Projection(
            Projection::new(
                42,
                LogicalPlan::synthetic(child),
                vec![
                    Expression::Reference(paro_planner::expression::ReferenceExpression {
                        index: 0,
                        return_type: LogicalType::Integer,
                    }),
                    Expression::Reference(paro_planner::expression::ReferenceExpression {
                        index: 1,
                        return_type: LogicalType::Integer,
                    }),
                ],
            )
            .with_visible_names(vec!["id_alias".to_string(), "score_alias".to_string()]),
        )
    }

    fn create_volatile_projection(child: LogicalOperator) -> LogicalOperator {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        LogicalOperator::Projection(Projection::new(
            42,
            LogicalPlan::synthetic(child),
            vec![Expression::Function(FunctionExpression::new(
                function,
                vec![],
                LogicalType::Double,
            ))],
        ))
    }

    #[test]
    fn test_can_optimize_simple() {
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(order),
            Some(create_constant_expr(10)),
            None,
        ));

        assert!(TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_can_optimize_with_offset() {
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(order),
            Some(create_constant_expr(10)),
            Some(create_constant_expr(5)),
        ));

        assert!(TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_no_limit() {
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(LogicalPlan::synthetic(order), None, None));

        assert!(!TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_no_order() {
        let get = create_test_get();
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(get),
            Some(create_constant_expr(10)),
            None,
        ));

        assert!(!TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_negative_limit() {
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(order),
            Some(create_constant_expr(-1)),
            None,
        ));

        assert!(!TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_optimize_negative_offset() {
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(order),
            Some(create_constant_expr(10)),
            Some(create_constant_expr(-5)),
        ));

        assert!(!TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_cannot_build_topn_through_volatile_projection() {
        let order = create_order_by(create_test_get());
        let projection = create_volatile_projection(order);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(10)),
            None,
        ));

        assert!(!TopNOptimizer::can_optimize(&limit));
    }

    #[test]
    fn test_optimize_simple() {
        let mut optimizer = TopNOptimizer::new();
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(order),
            Some(create_constant_expr(10)),
            None,
        ));

        let result = optimizer.optimize(limit);

        assert_eq!(result.op_type(), LogicalOperatorType::TopN);
        if let LogicalOperator::TopN(topn) = result {
            assert_eq!(topn.limit, 10);
            assert_eq!(topn.offset, 0);
            assert_eq!(topn.orders.len(), 1);
        } else {
            panic!("Expected TopN operator");
        }
    }

    #[test]
    fn test_optimize_with_offset() {
        let mut optimizer = TopNOptimizer::new();
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(order),
            Some(create_constant_expr(10)),
            Some(create_constant_expr(5)),
        ));

        let result = optimizer.optimize(limit);

        assert_eq!(result.op_type(), LogicalOperatorType::TopN);
        if let LogicalOperator::TopN(topn) = result {
            assert_eq!(topn.limit, 10);
            assert_eq!(topn.offset, 5);
            assert_eq!(topn.total_rows(), 15);
        } else {
            panic!("Expected TopN operator");
        }
    }

    #[test]
    fn test_optimize_propagates_hnsw_ef_hint() {
        let mut optimizer = TopNOptimizer::new();
        let get = create_test_get();
        let order = create_order_by(get);
        let limit = LogicalOperator::Limit(
            Limit::new(
                LogicalPlan::synthetic(order),
                Some(create_constant_expr(10)),
                None,
            )
            .with_hnsw_ef_hint(Some(256)),
        );

        let result = optimizer.optimize(limit);

        assert_eq!(result.op_type(), LogicalOperatorType::TopN);
        if let LogicalOperator::TopN(topn) = result {
            assert_eq!(topn.hnsw_ef_hint, Some(256));
        } else {
            panic!("Expected TopN operator");
        }
    }

    #[test]
    fn test_optimize_through_projection_preserves_output_names() {
        let mut optimizer = TopNOptimizer::new();
        let get = create_test_get();
        let order = create_order_by(get);
        let projection = create_projection(order);
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(projection),
            Some(create_constant_expr(3)),
            None,
        ));

        let result = optimizer.optimize(limit);

        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        assert_eq!(result.output_names(), vec!["id_alias", "score_alias"]);
        let LogicalOperator::Projection(proj) = result else {
            panic!("Expected Projection above TopN");
        };
        assert_eq!(proj.child.operator.op_type(), LogicalOperatorType::TopN);
        if let LogicalOperator::TopN(topn) = &proj.child.operator {
            assert_eq!(topn.limit, 3);
            assert_eq!(topn.offset, 0);
        } else {
            panic!("Expected TopN child");
        }
    }

    #[test]
    fn test_no_optimization_when_not_applicable() {
        let mut optimizer = TopNOptimizer::new();
        let get = create_test_get();
        let limit = LogicalOperator::Limit(Limit::new(
            LogicalPlan::synthetic(get),
            Some(create_constant_expr(10)),
            None,
        ));

        let result = optimizer.optimize(limit);

        // Should remain as Limit since there's no ORDER BY
        assert_eq!(result.op_type(), LogicalOperatorType::Limit);
    }
}
