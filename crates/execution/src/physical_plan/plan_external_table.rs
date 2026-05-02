// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
use paro_planner::operator::external_project::ExternalCostEstimate;
use paro_planner::operator::external_table::LogicalExternalTable;

use crate::operator::external::python_process_bridge::build_table_runtime_bridge;
use crate::operator::external::runtime_bridge::ExternalRoutineDescriptor;
use crate::operator::external::table::ExternalTable;
use crate::operator::scan::dummy_scan::PhysicalDummyScan;
use crate::operator::PhysicalOperator;

use super::generator::PhysicalPlanGenerator;

#[derive(Debug, Clone)]
pub struct ExternalTablePlanBinding {
    pub routine: ExternalRoutineDescriptor,
    pub worker_output_types: Vec<LogicalType>,
    pub emitted_output_types: Vec<LogicalType>,
    pub argument_count: usize,
    pub lateral: bool,
    pub parameterized: bool,
    pub estimated_cardinality: usize,
    pub cost: ExternalCostEstimate,
}

impl PhysicalPlanGenerator {
    pub fn create_plan_external_table(
        &self,
        external: &LogicalExternalTable,
        child: Option<Arc<dyn PhysicalOperator>>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let child = child.unwrap_or_else(|| Arc::new(PhysicalDummyScan::new()));
        let child_rows = child.estimated_cardinality().max(1);
        let estimated_cardinality = child_rows.saturating_mul(4);
        let argument_count = external
            .call
            .spec
            .as_ref()
            .map(|spec| spec.arguments.len())
            .or(match &external.call_expression {
                Expression::Function(function) => Some(function.children.len()),
                _ => None,
            })
            .unwrap_or(0);
        let passthrough_types = if external.parameterized && child.types().len() > argument_count {
            child.types()[argument_count..].to_vec()
        } else {
            Vec::new()
        };
        let worker_output_types = external.returned_types.clone();
        let mut emitted_output_types = worker_output_types.clone();
        emitted_output_types.extend(passthrough_types);
        let binding = ExternalTablePlanBinding {
            routine: ExternalRoutineDescriptor {
                label: table_expression_label(&external.call_expression, "__external_table"),
                identity: external.call.identity.clone(),
                semantics: external.call.semantics.clone(),
            },
            worker_output_types: worker_output_types.clone(),
            emitted_output_types,
            argument_count,
            lateral: external.lateral,
            parameterized: external.parameterized,
            estimated_cardinality,
            cost: external.cost,
        };
        let bridge = build_table_runtime_bridge(
            &self.context,
            &external.call,
            &binding.routine,
            &binding.worker_output_types,
        )?;

        Ok(Arc::new(ExternalTable::new(
            binding,
            child,
            Arc::new(bridge),
        )))
    }
}

fn table_expression_label(expression: &Expression, fallback: &str) -> String {
    match expression {
        Expression::Function(function) => function.function.name.clone(),
        _ => fallback.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator_type::PhysicalOperatorType;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external::routine::bound::BoundRoutineCallMeta;
    use paro_external::routine::boundary::{ExecutionBoundary, PlacementClass};
    use paro_external::routine::identity::RoutineCallIdentity;
    use paro_external::routine::spec::{
        RoutineId, RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability,
        RowSemantics,
    };
    use paro_function::scalar::ScalarFunction;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::{
        external_project::ExternalCostEstimate, external_table::LogicalExternalTable,
        ExpressionGet, LogicalOperator,
    };
    use paro_planner::plan::LogicalPlan;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn expression_get(ctx: &BindContext, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn table_call_expression() -> (Expression, BoundRoutineCallMeta) {
        fn passthrough(
            input: &Chunk,
            _ctx: &dyn paro_function::scalar::FunctionExecContext,
            result: &mut paro_common::vector::Vector,
        ) -> paro_common::error::Result<()> {
            let column = input.column(0).expect("input column");
            for row_idx in 0..input.size() {
                result.set_i32(row_idx, column.get_i32(row_idx).expect("non-null"));
            }
            Ok(())
        }

        let semantics = RoutineSemantics {
            stability: RoutineStability::Stable,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::None,
            row_semantics: RowSemantics::RelationExpanding,
            may_block: true,
        };
        let routine_meta = BoundRoutineCallMeta {
            identity: RoutineCallIdentity::Catalog {
                routine_id: RoutineId::from_raw(88),
                generation: 4,
            },
            semantics: semantics.clone(),
            boundary: ExecutionBoundary {
                placement: PlacementClass::External,
                may_block: true,
                row_semantics: RowSemantics::RelationExpanding,
            },
            spec: None,
        };
        let expression = Expression::Function(
            FunctionExpression::new(
                ScalarFunction::new(
                    "py_expand".to_string(),
                    vec![LogicalType::Integer],
                    LogicalType::Integer,
                    passthrough,
                ),
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
                LogicalType::Integer,
            )
            .with_routine_meta(routine_meta.clone()),
        );
        (expression, routine_meta)
    }

    #[test]
    fn create_plan_external_table_builds_sink_source_operator() {
        let generator = PhysicalPlanGenerator::new(test_session());
        let ctx = BindContext::new();
        let child = expression_get(&ctx, 3);
        let (call_expression, call_meta) = table_call_expression();
        let mut logical = LogicalPlan::new(
            &ctx,
            LogicalOperator::ExternalTable(
                LogicalExternalTable::new(
                    6,
                    vec!["value".to_string()],
                    vec![LogicalType::Integer],
                    call_expression,
                    call_meta,
                )
                .with_child(child, true, true)
                .with_cost(ExternalCostEstimate {
                    startup_cost: 3.0,
                    per_row_cost: 0.3,
                    bytes_cost: 0.07,
                    queue_risk: 0.6,
                }),
            ),
        );

        let physical = generator.plan(&mut logical).expect("plan should succeed");
        assert_eq!(
            physical.operator_type(),
            PhysicalOperatorType::ExternalTable
        );
        assert!(physical.is_sink());
        assert!(physical.is_source());
        assert!(!physical.parallel_source());
        assert_eq!(physical.types(), &[LogicalType::Integer]);

        let explain = physical.explain_params().join("\n");
        assert!(explain.contains("Routine: py_expand[88@4]"));
        assert!(explain.contains("Correlation: lateral=true parameterized=true"));
        assert!(explain.contains("Cost: startup=3.000"));
    }

    #[test]
    fn table_expression_label_prefers_function_name() {
        let (expression, _) = table_call_expression();
        assert_eq!(table_expression_label(&expression, "fallback"), "py_expand");

        let non_function = Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer));
        assert_eq!(
            table_expression_label(&non_function, "fallback"),
            "fallback"
        );
    }
}
