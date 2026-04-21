// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::error::Result;
use paro_planner::expression::Expression;
use paro_planner::operator::external_project::{ExternalCostEstimate, LogicalExternalProject};

use crate::operator::external::project::ExternalProject;
use crate::operator::external::python_process_bridge::build_project_runtime_bridge;
use crate::operator::external::runtime_bridge::ExternalRoutineDescriptor;
use crate::operator::PhysicalOperator;

use super::generator::PhysicalPlanGenerator;

#[derive(Debug, Clone)]
pub struct ExternalProjectPlanBinding {
    pub routines: Vec<ExternalRoutineDescriptor>,
    pub expressions: Vec<paro_planner::operator::external_project::ExternalProjectExpression>,
    pub cost: ExternalCostEstimate,
}

impl PhysicalPlanGenerator {
    pub fn create_plan_external_project(
        &self,
        external: &LogicalExternalProject,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let routines = external
            .expressions
            .iter()
            .map(|expression| ExternalRoutineDescriptor {
                label: project_expression_label(&expression.expression, &expression.output_name),
                identity: expression.routine_meta.identity.clone(),
                semantics: expression.routine_meta.semantics.clone(),
            })
            .collect();
        let binding = ExternalProjectPlanBinding {
            routines,
            expressions: external.expressions.clone(),
            cost: external.cost,
        };
        let bridge =
            build_project_runtime_bridge(&self.context, &binding.routines, &binding.expressions)?;
        Ok(Arc::new(ExternalProject::new(
            binding,
            child,
            Arc::new(bridge),
        )))
    }
}

fn project_expression_label(expression: &Expression, fallback: &str) -> String {
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
    use paro_function::scalar::ScalarFunction;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::{
        external_project::{
            ExternalCostEstimate, ExternalProjectExpression, LogicalExternalProject,
        },
        ExpressionGet, LogicalOperator,
    };
    use paro_planner::plan::LogicalPlan;
    use paro_routine::{
        BoundRoutineCallMeta, ExecutionBoundary, PlacementClass, RoutineCallIdentity, RoutineId,
        RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics,
    };
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

    fn external_expression() -> ExternalProjectExpression {
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
            stability: RoutineStability::Immutable,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::None,
            row_semantics: RowSemantics::RowPreserving,
            may_block: true,
        };
        let routine_meta = BoundRoutineCallMeta {
            identity: RoutineCallIdentity::Catalog {
                routine_id: RoutineId::from_raw(41),
                generation: 9,
            },
            semantics: semantics.clone(),
            boundary: ExecutionBoundary {
                placement: PlacementClass::External,
                may_block: true,
                row_semantics: RowSemantics::RowPreserving,
            },
            spec: None,
        };
        ExternalProjectExpression {
            output_name: "__ext".to_string(),
            expression: Expression::Function(
                FunctionExpression::new(
                    ScalarFunction::new(
                        "py_score".to_string(),
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
            ),
            routine_meta,
        }
    }

    #[test]
    fn create_plan_external_project_builds_external_operator() {
        let generator = PhysicalPlanGenerator::new(test_session());
        let ctx = BindContext::new();
        let child = expression_get(&ctx, 7);
        let mut logical = LogicalPlan::new(
            &ctx,
            LogicalOperator::ExternalProject(
                LogicalExternalProject::new(5, child, vec![external_expression()]).with_cost(
                    ExternalCostEstimate {
                        startup_cost: 2.0,
                        per_row_cost: 0.2,
                        bytes_cost: 0.05,
                        queue_risk: 0.4,
                    },
                ),
            ),
        );

        let physical = generator.plan(&mut logical).expect("plan should succeed");
        assert_eq!(
            physical.operator_type(),
            PhysicalOperatorType::ExternalProject
        );
        assert_eq!(
            physical.types(),
            &[LogicalType::Integer, LogicalType::Integer]
        );

        let explain = physical.explain_params().join("\n");
        assert!(explain.contains("Routines: py_score[41@9]"));
        assert!(explain.contains("Cost: startup=2.000"));
    }

    #[test]
    fn project_expression_label_prefers_function_name() {
        let expression = external_expression();
        assert_eq!(
            project_expression_label(&expression.expression, "fallback"),
            "py_score"
        );

        let non_function = Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer));
        assert_eq!(
            project_expression_label(&non_function, "fallback"),
            "fallback"
        );
    }
}
