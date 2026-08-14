// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, TableCatalogEntry};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_external::routine::spec::{
    RoutineId, RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability,
    RowSemantics,
};
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::window::WindowFunction;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::{CTEMaterialize, OrderByNode};
use paro_planner::expression::{
    AggregateExpression, ConstantExpression, Expression, OrderByExpression, ReferenceExpression,
    WindowExpression, WindowFrame,
};
use paro_planner::operator::external_project::ExternalCostEstimate;
use paro_planner::operator::join::{
    ComparisonJoin, Join, JoinComparisonType, JoinCondition, JoinType,
};
use paro_planner::operator::{
    Aggregate as LogicalAggregate, CTERef, DelimGet, Distinct, EmptyResult, ExpressionGet, Filter,
    Limit, LogicalOperator, MaterializedCTE, Order as LogicalOrder, Projection, RecursiveCTE,
    TopN as LogicalTopN, Window as LogicalWindow,
};
use paro_planner::plan::{LogicalPlan, PlanNodeId};
use paro_storage::table::table_factory::TableFactory;

use crate::operators::external::runtime_bridge::{
    ExternalRoutineDescriptor, ExternalRuntimeBridge,
};
use crate::physical::children::{PlanChildren, PlanChildrenArena};
use crate::physical::generator::{PhysicalPlanGenerator, PlanBuildContext};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::node::{OperatorLabel, PhysicalPlanNode};
use crate::physical::plan::{PhysicalPlan, PhysicalPlanNodeArena};
use crate::physical::properties::{
    BatchIndexRequirement, MorselCapability, MorselPartitioning, OrderingColumn, OrderingDirection,
    OrderingRequirement, OrderingSpec, PartitioningRequirement, Placement, PlanPropertyMap,
    PropertyRepairKind,
};
use crate::physical::specs::{ExternalProjectSpec, ExternalTableSpec, PhysicalNodeKind};
use crate::physical::{RowType, RowsetScanSpec};

use super::super::graph::{
    ClientResultSpec, ControlRegion, ControlRegionId, DelimJoinSide, DependencyKind,
    MaterializedSourceSpec, PipelineDependency, PipelineSubgraphRoot, SharedSinkId, SinkSharing,
    SinkSpec, SourceSpec,
};
use super::super::handles::{BreakerHandleCatalogBuilder, BreakerHandleKind};
use super::*;

fn linear_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let filter = LogicalPlan::new(&ctx, LogicalOperator::Filter(Filter::new(values, vec![])));
    let project_expr = Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer));
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, filter, vec![project_expr]).with_output_names(vec!["a".into()]),
        ),
    );
    let limit = LogicalPlan::new(
        &ctx,
        LogicalOperator::Limit(Limit::new(project, None, None)),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&limit).unwrap()
}

fn projection_changes_schema_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string(), "b".to_string()],
            vec![LogicalType::Integer, LogicalType::Varchar],
        )),
    );
    let project_expr = Expression::Reference(ReferenceExpression::new(1, LogicalType::Varchar));
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, values, vec![project_expr]).with_output_names(vec!["b".to_string()]),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&project).unwrap()
}

fn grouped_aggregate_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let aggregate = LogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(LogicalAggregate::new(
            1,
            2,
            3,
            values,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                Vec::new(),
                LogicalType::BigInt,
            ))],
            Vec::new(),
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&aggregate).unwrap()
}

fn aggregate_probe_hash_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let aggregate_input = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["k".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let aggregate = LogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(LogicalAggregate::new(
            1,
            2,
            3,
            aggregate_input,
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                Vec::new(),
                LogicalType::BigInt,
            ))],
            Vec::new(),
        )),
    );
    let build = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            4,
            vec![],
            vec!["k".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            aggregate,
            build,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            )],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn ungrouped_aggregate_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let aggregate = LogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(LogicalAggregate::new(
            1,
            2,
            3,
            values,
            Vec::new(),
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                Vec::new(),
                LogicalType::BigInt,
            ))],
            Vec::new(),
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    let generated = generator.generate(&aggregate).unwrap();
    let PhysicalNodeKind::Aggregate(spec) = &generated.node(generated.root).kind else {
        panic!("expected ungrouped aggregate root");
    };

    let mut nodes = PhysicalPlanNodeArena::default();
    let mut children = PlanChildrenArena::default();
    let child = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
        cardinality: None,
        kind: PhysicalNodeKind::RowsetScan(rowset_spec_for_test()),
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "ROWSET_SCAN"),
    });
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: generated.node(generated.root).output.clone(),
        cardinality: None,
        kind: PhysicalNodeKind::Aggregate(spec.clone()),
        children: children.pack(vec![child]),
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "UNGROUPED_AGGREGATE"),
    });
    PhysicalPlan::new(root, nodes, children, PlanPropertyMap::default())
}

fn topn_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let order = paro_planner::binder::ir::OrderByNode {
        expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        ascending: true,
        nulls_first: false,
    };
    let topn = LogicalPlan::new(
        &ctx,
        LogicalOperator::TopN(LogicalTopN::new(values, vec![order], 2, 0)),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&topn).unwrap()
}

fn topn_probe_hash_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["k".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let order = OrderByNode {
        expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        ascending: true,
        nulls_first: false,
    };
    let topn = LogicalPlan::new(
        &ctx,
        LogicalOperator::TopN(LogicalTopN::new(values, vec![order], 2, 0)),
    );
    let build = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["k".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            topn,
            build,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            )],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn hash_join_plan(join_type: JoinType) -> crate::physical::PhysicalPlan {
    hash_join_plan_with_context(join_type, PlanBuildContext::default())
}

fn hash_join_plan_with_context(
    join_type: JoinType,
    build_context: PlanBuildContext,
) -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["lk".to_string(), "lv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["rk".to_string(), "rv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(join_type, left, right, vec![condition])),
    );

    let mut generator = PhysicalPlanGenerator::new(build_context);
    generator.generate(&join).unwrap()
}

fn nested_loop_join_plan(join_type: JoinType) -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["lk".to_string(), "lv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["rk".to_string(), "rv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        JoinComparisonType::LessThan,
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(join_type, left, right, vec![condition])),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn sort_range_join_plan(join_type: JoinType) -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["l1".to_string(), "l2".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["r1".to_string(), "r2".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let conditions = vec![
        JoinCondition::new(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            JoinComparisonType::LessThan,
        ),
        JoinCondition::new(
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
            JoinComparisonType::GreaterThan,
        ),
    ];
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(join_type, left, right, conditions)),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn project_above_nested_loop_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["lk".to_string(), "lv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["rk".to_string(), "rv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        JoinComparisonType::LessThan,
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![condition],
        )),
    );
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                2,
                join,
                vec![Expression::Reference(ReferenceExpression::new(
                    1,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["lv".to_string()]),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&project).unwrap()
}

fn limit_above_right_nested_loop_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["lk".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["rk".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        JoinComparisonType::LessThan,
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Right,
            left,
            right,
            vec![condition],
        )),
    );
    let limit = LogicalPlan::new(
        &ctx,
        LogicalOperator::Limit(Limit::new(
            join,
            Some(Expression::Constant(ConstantExpression::new(
                Value::Integer(10),
                LogicalType::Integer,
            ))),
            None,
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&limit).unwrap()
}

fn left_deep_right_nested_loop_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let make_values = |table_index, key: &str| {
        LogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![],
                vec![key.to_string()],
                vec![LogicalType::Integer],
            )),
        )
    };
    let a = make_values(0, "ak");
    let b = make_values(1, "bk");
    let c = make_values(2, "ck");
    let nlj_condition = JoinCondition::new(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        JoinComparisonType::LessThan,
    );
    let right_nlj = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(JoinType::Right, a, b, vec![nlj_condition])),
    );
    let hash_condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            right_nlj,
            c,
            vec![hash_condition],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn cross_product_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["l".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["r".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let join = LogicalPlan::new(&ctx, LogicalOperator::Join(Join::cross(left, right)));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn hash_join_with_projected_cross_product_probe_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let a = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["ak".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let b = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["bk".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let c = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            2,
            vec![],
            vec!["ck".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let cross = LogicalPlan::new(&ctx, LogicalOperator::Join(Join::cross(a, b)));
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                3,
                cross,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["ak".to_string()]),
        ),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            project,
            c,
            vec![condition],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&join).unwrap()
}

fn aggregate_above_right_anti_hash_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["lk".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["rk".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::RightAnti,
            left,
            right,
            vec![condition],
        )),
    );
    let aggregate = LogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(LogicalAggregate::new(
            2,
            3,
            4,
            join,
            Vec::new(),
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                Vec::new(),
                LogicalType::BigInt,
            ))],
            Vec::new(),
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&aggregate).unwrap()
}

fn materialized_cte_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let cte_query = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let cte_ref = LogicalPlan::new(
        &ctx,
        LogicalOperator::CTERef(CTERef::new(
            7,
            1,
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let cte = LogicalPlan::new(
        &ctx,
        LogicalOperator::MaterializedCTE(
            MaterializedCTE::new(
                7,
                "nums".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
                CTEMaterialize::Materialized,
                cte_query,
                cte_ref,
            )
            .with_ref_count(1),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&cte).unwrap()
}

fn recursive_cte_plan(union_all: bool) -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let cte = recursive_cte_logical_plan(&ctx, union_all);

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&cte).unwrap()
}

fn recursive_cte_with_invariant_hash_build_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let anchor = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["node".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let recursive_ref = LogicalPlan::new(
        &ctx,
        LogicalOperator::CTERef(CTERef::new(
            9,
            1,
            vec!["node".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let edges = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["src".to_string(), "dst".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            recursive_ref,
            edges,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            )],
        )),
    );
    let recursive = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                2,
                join,
                vec![Expression::Reference(ReferenceExpression::new(
                    2,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["node".to_string()]),
        ),
    );
    let cte = LogicalPlan::new(
        &ctx,
        LogicalOperator::RecursiveCTE(RecursiveCTE {
            cte_index: 9,
            cte_name: "walk".to_string(),
            column_names: vec!["node".to_string()],
            column_types: vec![LogicalType::Integer],
            union_all: true,
            anchor: Box::new(anchor),
            recursive: Box::new(recursive),
        }),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&cte).unwrap()
}

fn projected_recursive_cte_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let cte = recursive_cte_logical_plan(&ctx, true);
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                2,
                cte,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["v".to_string()]),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&project).unwrap()
}

fn ordered_recursive_cte_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let cte = recursive_cte_logical_plan(&ctx, true);
    let order = LogicalPlan::new(
        &ctx,
        LogicalOperator::Order(LogicalOrder::new(
            cte,
            vec![paro_planner::binder::ir::OrderByNode {
                expression: Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                )),
                ascending: true,
                nulls_first: false,
            }],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&order).unwrap()
}

fn recursive_cte_logical_plan(ctx: &BindContext, union_all: bool) -> LogicalPlan {
    let anchor = LogicalPlan::new(
        ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let recursive_ref = LogicalPlan::new(
        ctx,
        LogicalOperator::CTERef(CTERef::new(
            9,
            1,
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    LogicalPlan::new(
        ctx,
        LogicalOperator::RecursiveCTE(RecursiveCTE {
            cte_index: 9,
            cte_name: "nums".to_string(),
            column_names: vec!["v".to_string()],
            column_types: vec![LogicalType::Integer],
            union_all,
            anchor: Box::new(anchor),
            recursive: Box::new(recursive_ref),
        }),
    )
}

fn left_delim_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["l".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer])),
    );
    let mut join = ComparisonJoin::new(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    join.duplicate_eliminated_columns = vec![Expression::Reference(ReferenceExpression::new(
        0,
        LogicalType::Integer,
    ))];
    let plan = LogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(join)));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&plan).unwrap()
}

fn hash_join_with_delim_probe_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let capture = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["capture".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let dependent = LogicalPlan::new(
        &ctx,
        LogicalOperator::DelimGet(DelimGet::new(99, vec![LogicalType::Integer])),
    );
    let mut delim_join = ComparisonJoin::new(
        JoinType::Inner,
        capture,
        dependent,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    delim_join.duplicate_eliminated_columns = vec![Expression::Reference(
        ReferenceExpression::new(0, LogicalType::Integer),
    )];
    let delim_probe = LogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(delim_join)));
    let probe = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                2,
                delim_probe,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["projected_capture".to_string()]),
        ),
    );
    let build = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["build".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let outer = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            probe,
            build,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            )],
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&outer).unwrap()
}

fn left_delim_join_with_recursive_dependent_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["l".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = recursive_cte_logical_plan(&ctx, true);
    let mut join = ComparisonJoin::new(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    join.duplicate_eliminated_columns = vec![Expression::Reference(ReferenceExpression::new(
        0,
        LogicalType::Integer,
    ))];
    let plan = LogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(join)));

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&plan).unwrap()
}

fn projection_above_hash_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let left = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["lk".to_string(), "lv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["rk".to_string(), "rv".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![condition],
        )),
    );
    let project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                2,
                join,
                vec![Expression::Reference(ReferenceExpression::new(
                    1,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["lv".to_string()]),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&project).unwrap()
}

fn left_deep_hash_join_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let make_values = |table_index, key: &str, value: &str| {
        LogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![],
                vec![key.to_string(), value.to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        )
    };
    let a = make_values(0, "ak", "av");
    let b = make_values(1, "bk", "bv");
    let c = make_values(2, "ck", "cv");
    let condition = || {
        JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )
    };
    let ab = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(JoinType::Inner, a, b, vec![condition()])),
    );
    let abc = LogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(JoinType::Inner, ab, c, vec![condition()])),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&abc).unwrap()
}

fn order_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let order = paro_planner::binder::ir::OrderByNode {
        expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        ascending: true,
        nulls_first: false,
    };
    let order = LogicalPlan::new(
        &ctx,
        LogicalOperator::Order(LogicalOrder::new(values, vec![order])),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&order).unwrap()
}

fn order_with_final_projection_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string(), "b".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let hidden_project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                1,
                values,
                vec![
                    Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                    Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer)),
                ],
            )
            .with_output_names(vec!["a".to_string()]),
        ),
    );
    let order = paro_planner::binder::ir::OrderByNode {
        expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        ascending: true,
        nulls_first: false,
    };
    let order = LogicalPlan::new(
        &ctx,
        LogicalOperator::Order(LogicalOrder::new(hidden_project, vec![order])),
    );
    let final_project = LogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                2,
                order,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
            )
            .with_output_names(vec!["a".to_string()]),
        ),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&final_project).unwrap()
}

fn partitioned_window_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["grp".to_string(), "v".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            1,
            vec![WindowExpression::native(
                WindowFunction::rank(),
                Vec::new(),
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
                vec![OrderByExpression {
                    expression: Expression::Reference(ReferenceExpression::new(
                        1,
                        LogicalType::Integer,
                    )),
                    ascending: true,
                    nulls_first: false,
                }],
                WindowFrame::default(),
                false,
            )],
            values,
        )),
    );

    let mut generator = PhysicalPlanGenerator::new(PlanBuildContext::default());
    generator.generate(&window).unwrap()
}

fn partition_aggregate_window_plan() -> crate::physical::PhysicalPlan {
    let ctx = BindContext::new();
    let values = LogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["grp".to_string(), "v".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    let aggregate =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    let window = LogicalPlan::new(
        &ctx,
        LogicalOperator::Window(LogicalWindow::new(
            1,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );

    PhysicalPlanGenerator::new(PlanBuildContext::default())
        .generate(&window)
        .unwrap()
}

fn rowset_spec_for_test() -> RowsetScanSpec {
    let table = Arc::new(TableCatalogEntry::new(
        "test".to_string(),
        "main".to_string(),
        "t".to_string(),
        vec![ColumnDefinition::new("a".to_string(), LogicalType::Integer)],
        Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .unwrap(),
        ),
        CatalogObjectId::from_raw(10_001),
        0,
    ));

    RowsetScanSpec {
        table_index: 0,
        output_names: vec!["a".to_string()].into_boxed_slice(),
        returned_types: vec![LogicalType::Integer].into_boxed_slice(),
        relation_name: Some("t".to_string()),
        relation_alias: None,
        column_projection: crate::physical::specs::RowsetColumnProjection::new(vec![0]),
        emit_row_id: false,
        column_types: vec![LogicalType::Integer].into_boxed_slice(),
        table,
        predicate: None,
        residual_predicates: Vec::new().into_boxed_slice(),
        access_policy: crate::physical::specs::RowsetScanAccessPolicy::new(
            true,
            None,
            Default::default(),
        ),
        scan_order: None,
        runtime_filter_expressions: Vec::new().into_boxed_slice(),
    }
}

fn external_project_plan() -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    let mut children = PlanChildrenArena::default();
    let child = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
        cardinality: None,
        kind: PhysicalNodeKind::DummyScan(crate::physical::specs::DummyScanSpec),
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "DUMMY_SCAN"),
    });
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
        cardinality: None,
        kind: PhysicalNodeKind::ExternalProject(ExternalProjectSpec {
            routines: vec![external_routine_descriptor(RowSemantics::RowPreserving)]
                .into_boxed_slice(),
            expressions: Vec::new().into_boxed_slice(),
            cost: ExternalCostEstimate::default(),
            bridge: Arc::new(ExternalRuntimeBridge::default_bridge()),
            input_names: vec!["a".to_string()].into_boxed_slice(),
            input_types: vec![LogicalType::Integer].into_boxed_slice(),
            output_names: vec!["a".to_string()].into_boxed_slice(),
            output_types: vec![LogicalType::Integer].into_boxed_slice(),
        }),
        children: children.pack(vec![child]),
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, "EXTERNAL_PROJECT"),
    });

    PhysicalPlan::new(root, nodes, children, PlanPropertyMap::default())
}

fn external_table_plan() -> PhysicalPlan {
    external_single_node_plan(
        PhysicalNodeKind::ExternalTable(ExternalTableSpec {
            routine: external_routine_descriptor(RowSemantics::RelationExpanding),
            worker_output_types: vec![LogicalType::Integer].into_boxed_slice(),
            emitted_output_types: vec![LogicalType::Integer].into_boxed_slice(),
            argument_count: 0,
            lateral: false,
            parameterized: false,
            estimated_cardinality: 1,
            cost: ExternalCostEstimate::default(),
            bridge: Arc::new(ExternalRuntimeBridge::default_bridge()),
        }),
        "EXTERNAL_TABLE",
        RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
    )
}

fn external_single_node_plan(
    kind: PhysicalNodeKind,
    display_name: &'static str,
    output: RowType,
) -> PhysicalPlan {
    let mut nodes = PhysicalPlanNodeArena::default();
    let root = nodes.push(PhysicalPlanNode {
        id: PhysicalPlanNodeId::INVALID,
        output,
        cardinality: None,
        kind,
        children: PlanChildren::Empty,
        label: OperatorLabel::new(PlanNodeId::SYNTHETIC, display_name),
    });

    PhysicalPlan::new(
        root,
        nodes,
        PlanChildrenArena::default(),
        PlanPropertyMap::default(),
    )
}

fn external_routine_descriptor(row_semantics: RowSemantics) -> ExternalRoutineDescriptor {
    ExternalRoutineDescriptor {
        label: "external_test".to_string(),
        identity: RoutineCallIdentity::Catalog {
            routine_id: RoutineId::from_raw(7),
            generation: 1,
        },
        semantics: RoutineSemantics {
            stability: RoutineStability::Volatile,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::HasSideEffects,
            row_semantics,
            may_block: true,
        },
        spec: None,
    }
}

#[path = "basic_tests.rs"]
mod basic_tests;
#[path = "cte_tests.rs"]
mod cte_tests;
#[path = "guard_tests.rs"]
mod guard_tests;
#[path = "join_order_tests.rs"]
mod join_order_tests;
