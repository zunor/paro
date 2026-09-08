// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::minmax::get_max_function;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::aggregate::{AggregateComparison, AggregateFinalizeProjection};
use paro_function::scalar::cast::decimal_casts::bind_decimal_casts;
use paro_function::scalar::cast::CastFunctionSet;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, CastExpression, ColumnRefExpression, ComparisonExpression,
    ComparisonType, ConjunctionExpression, ConjunctionType, ConstantExpression, Expression,
    ReferenceExpression,
};
use paro_planner::operator::aggregate::GroupDependency;
use paro_planner::operator::{
    Aggregate, ColumnBinding, ExpressionGet, Filter, LogicalOperator, PostAggregateReduction,
};
use paro_planner::plan::OwnedLogicalPlan;
use paro_storage::statistics::NumericStats;

use super::*;

#[test]
fn aggregate_lowers_post_reduction_into_separate_local_reference_domains() {
    let ctx = BindContext::new();
    let values = values(&ctx, vec![LogicalType::Integer]);
    let count = Expression::Aggregate(Box::new(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    )));
    let (max_function, _) = get_max_function()
        .bind(&[LogicalType::BigInt])
        .expect("bind max(bigint)");
    let reducer = Expression::Aggregate(Box::new(AggregateExpression::new(
        max_function,
        vec![aggregate_column(0, LogicalType::BigInt)],
        LogicalType::BigInt,
    )));
    let aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![reference(0, LogicalType::Integer)],
        vec![],
        vec![count],
        vec![],
    )
    .with_post_reduction(PostAggregateReduction {
        reduction_index: 4,
        reducers: vec![reducer],
        scalar_expressions: vec![reference(0, LogicalType::BigInt)],
        predicate: comparison(
            ComparisonType::Equal,
            aggregate_column(0, LogicalType::BigInt),
            scalar_column(0, LogicalType::BigInt),
        ),
    });
    assert_eq!(aggregate.returned_types.len(), 2);
    assert_eq!(aggregate.get_column_bindings().len(), 2);

    let spec = lower_aggregate(&ctx, aggregate, ExtractionContext::default(), false);
    assert_eq!(
        spec.output_types.len(),
        2,
        "hidden scalar is not SQL output"
    );
    let reduction = spec.post_reduction.as_ref().expect("post reduction");
    assert!(reduction.input_rollup_sources.is_none());
    assert!(matches!(
        &reduction.reducers[0],
        Expression::Aggregate(reducer)
            if matches!(&reducer.children[0], Expression::Reference(reference) if reference.index == 0)
    ));
    assert!(matches!(
        &reduction.scalar_expressions[0],
        Expression::Reference(reference) if reference.index == 0
    ));
    assert!(matches!(
        &reduction.predicate,
        Expression::Comparison(predicate)
            if matches!(predicate.left.as_ref(), Expression::Reference(reference) if reference.index == 0)
                && matches!(predicate.right.as_ref(), Expression::Reference(reference) if reference.index == 1)
    ));
}

#[test]
fn integer_sum_candidate_without_direct_state_predicate_uses_preserving_fallback() {
    let ctx = BindContext::new();
    let aggregate = integer_sum_reduction(&ctx);
    let spec = lower_aggregate(&ctx, aggregate, parallel_context(), false);
    assert!(spec.perfect_hash.is_none());
    assert!(spec
        .post_reduction
        .as_ref()
        .unwrap()
        .input_rollup_sources
        .is_none());
}

#[test]
fn q11_snapshot_bounds_do_not_admit_a_cacheable_perfect_domain() {
    let ctx = BindContext::new();
    let aggregate = decimal_sum_reduction(&ctx, false);
    let spec = lower_aggregate(&ctx, aggregate, parallel_context(), false);
    let post = spec.post_reduction.as_ref().unwrap();
    assert!(spec.perfect_hash.is_none());
    assert!(post.input_rollup_sources.is_none());
    assert_eq!(
        post.state_filter_plan(),
        Some(crate::physical::specs::PostAggregateStateFilterPlan {
            aggregate_index: 0,
            scalar_index: 0,
            projection: AggregateFinalizeProjection::DecimalCast {
                target_precision: 38,
                target_scale: 12,
                try_cast: false,
            },
            comparison: AggregateComparison::GreaterThan,
        })
    );
}

#[test]
fn scalar_left_q11_predicate_preserves_comparison_orientation() {
    let ctx = BindContext::new();
    let mut aggregate = decimal_sum_reduction(&ctx, false);
    let post = aggregate.post_reduction.as_mut().unwrap();
    let Expression::Comparison(predicate) = &mut post.predicate else {
        unreachable!()
    };
    std::mem::swap(&mut predicate.left, &mut predicate.right);
    predicate.comparison_type = ComparisonType::LessThan;

    let spec = lower_aggregate(&ctx, aggregate, parallel_context(), false);
    assert_eq!(
        spec.post_reduction
            .as_ref()
            .unwrap()
            .state_filter_plan()
            .unwrap()
            .comparison,
        AggregateComparison::GreaterThan
    );
}

#[test]
fn single_local_table_keeps_preserving_post_reduction() {
    let ctx = BindContext::new();
    let aggregate = decimal_sum_reduction(&ctx, false);
    let spec = lower_aggregate(&ctx, aggregate, ExtractionContext::default(), false);
    assert!(spec.perfect_hash.is_none());
    assert!(spec
        .post_reduction
        .as_ref()
        .unwrap()
        .input_rollup_sources
        .is_none());
}

#[test]
fn ordinary_having_keeps_preserving_post_reduction() {
    let ctx = BindContext::new();
    let aggregate = decimal_sum_reduction(&ctx, false);
    let spec = lower_aggregate(&ctx, aggregate, parallel_context(), true);
    assert_eq!(spec.having_filter.len(), 1);
    assert!(spec
        .post_reduction
        .as_ref()
        .unwrap()
        .input_rollup_sources
        .is_none());
}

#[test]
fn complex_post_predicate_keeps_preserving_post_reduction() {
    let ctx = BindContext::new();
    let aggregate = decimal_sum_reduction(&ctx, true);
    let spec = lower_aggregate(&ctx, aggregate, parallel_context(), false);
    assert!(spec
        .post_reduction
        .as_ref()
        .unwrap()
        .input_rollup_sources
        .is_none());
}

#[test]
fn physical_input_rollup_verifier_rejects_stale_payload_contract() {
    let ctx = BindContext::new();
    let mut spec = lower_aggregate(
        &ctx,
        decimal_sum_reduction(&ctx, false),
        parallel_context(),
        false,
    );
    force_input_rollup(&mut spec);
    spec.payload_types[1] = LogicalType::BigInt;
    let error = spec.verify_post_reduction().unwrap_err();
    assert!(error.to_string().contains("argument 0 type mismatch"));

    let mut spec = lower_aggregate(
        &ctx,
        decimal_sum_reduction(&ctx, false),
        parallel_context(),
        false,
    );
    force_input_rollup(&mut spec);
    spec.aggregate_inputs[0] = Box::new([usize::MAX]);
    let error = spec.verify_post_reduction().unwrap_err();
    assert!(error.to_string().contains("missing payload column"));
}

#[test]
fn physical_input_rollup_verifier_rejects_unexecutable_strategy() {
    let ctx = BindContext::new();
    let admitted = || {
        let mut spec = lower_aggregate(
            &ctx,
            decimal_sum_reduction(&ctx, false),
            parallel_context(),
            false,
        );
        force_input_rollup(&mut spec);
        spec
    };

    let mut spec = admitted();
    spec.perfect_hash
        .as_mut()
        .unwrap()
        .resource
        .memory
        .max_concurrent_tasks = 1;
    assert!(spec
        .verify_post_reduction()
        .unwrap_err()
        .to_string()
        .contains("multiple local perfect tables"));

    let mut spec = admitted();
    let Expression::Aggregate(source) = &mut spec.aggregates[0] else {
        unreachable!()
    };
    source.function.simple_update = None;
    assert!(spec
        .verify_post_reduction()
        .unwrap_err()
        .to_string()
        .contains("not a plain inline aggregate"));

    let mut spec = admitted();
    let Expression::Aggregate(reducer) = &mut spec.post_reduction.as_mut().unwrap().reducers[0]
    else {
        unreachable!()
    };
    reducer.aggr_type = AggregateType::Distinct;
    assert!(spec
        .verify_post_reduction()
        .unwrap_err()
        .to_string()
        .contains("must be non-distinct"));
}

fn force_input_rollup(spec: &mut crate::physical::specs::AggregateSpec) {
    spec.perfect_hash = Some(crate::physical::specs::PerfectHashAggregatePlan {
        group_minima: Box::new([i128::from(i32::MIN)]),
        group_cardinalities: Box::new([4]),
        resource: crate::physical::specs::PerfectHashResourceContract {
            slots: 4,
            table_bytes_upper: usize::MAX,
            memory: crate::physical::ExecutionMemoryContract {
                fixed_non_revocable_bytes: u64::MAX,
                max_concurrent_tasks: 2,
                ..Default::default()
            },
        },
    });
    spec.post_reduction
        .as_mut()
        .expect("post reduction")
        .input_rollup_sources = Some(Box::new([0]));
}

#[test]
fn post_reduction_disables_dependent_group_state_projection() {
    let ctx = BindContext::new();
    let values = values(&ctx, vec![LogicalType::Integer, LogicalType::Integer]);
    let count = Expression::Aggregate(Box::new(AggregateExpression::new(
        get_count_star_function(),
        vec![],
        LogicalType::BigInt,
    )));
    let (max_function, _) = get_max_function().bind(&[LogicalType::BigInt]).unwrap();
    let reducer = Expression::Aggregate(Box::new(AggregateExpression::new(
        max_function,
        vec![aggregate_column(0, LogicalType::BigInt)],
        LogicalType::BigInt,
    )));
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![
            reference(0, LogicalType::Integer),
            reference(1, LogicalType::Integer),
        ],
        vec![],
        vec![count],
        vec![],
    )
    .with_post_reduction(PostAggregateReduction {
        reduction_index: 4,
        reducers: vec![reducer],
        scalar_expressions: vec![reference(0, LogicalType::BigInt)],
        predicate: comparison(
            ComparisonType::Equal,
            aggregate_column(0, LogicalType::BigInt),
            scalar_column(0, LogicalType::BigInt),
        ),
    });
    aggregate.group_dependencies.push(GroupDependency {
        determinants: Box::new([0]),
        dependents: Box::new([1]),
    });

    let spec = lower_aggregate(&ctx, aggregate, ExtractionContext::default(), false);
    assert_eq!(spec.grouping_key_count, 2);
    assert_eq!(spec.aggregates.len(), 1);
    assert!(spec.state_output_projection.is_empty());
    assert!(spec.post_reduction.is_some());
}

fn integer_sum_reduction(ctx: &BindContext) -> Aggregate {
    let values = values(ctx, vec![LogicalType::Integer, LogicalType::Integer]);
    let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    let merge = sum.partial_merge_function().unwrap();
    let source = Expression::Aggregate(Box::new(AggregateExpression::new(
        sum,
        vec![reference(1, LogicalType::Integer)],
        LogicalType::BigInt,
    )));
    let reducer = Expression::Aggregate(Box::new(AggregateExpression::new(
        merge,
        vec![aggregate_column(0, LogicalType::BigInt)],
        LogicalType::BigInt,
    )));
    bounded_group(Aggregate::new(
        1,
        2,
        3,
        values,
        vec![reference(0, LogicalType::Integer)],
        vec![],
        vec![source],
        vec![],
    ))
    .with_post_reduction(PostAggregateReduction {
        reduction_index: 4,
        reducers: vec![reducer],
        scalar_expressions: vec![reference(0, LogicalType::BigInt)],
        predicate: comparison(
            ComparisonType::GreaterThan,
            aggregate_column(0, LogicalType::BigInt),
            scalar_column(0, LogicalType::BigInt),
        ),
    })
}

fn decimal_sum_reduction(ctx: &BindContext, complex_predicate: bool) -> Aggregate {
    let input_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let sum_type = LogicalType::Decimal {
        precision: 38,
        scale: 2,
    };
    let comparison_type = LogicalType::Decimal {
        precision: 38,
        scale: 12,
    };
    let values = values(ctx, vec![LogicalType::Integer, input_type.clone()]);
    let (sum, _) = get_sum_function()
        .bind(std::slice::from_ref(&input_type))
        .unwrap();
    let merge = sum.partial_merge_function().unwrap();
    let source = Expression::Aggregate(Box::new(AggregateExpression::new(
        sum,
        vec![reference(1, input_type)],
        sum_type.clone(),
    )));
    let reducer = Expression::Aggregate(Box::new(AggregateExpression::new(
        merge,
        vec![aggregate_column(0, sum_type.clone())],
        sum_type.clone(),
    )));
    let scalar = decimal_cast(reference(0, sum_type.clone()), comparison_type.clone());
    let comparison = comparison(
        ComparisonType::GreaterThan,
        decimal_cast(
            aggregate_column(0, sum_type.clone()),
            comparison_type.clone(),
        ),
        scalar_column(0, comparison_type),
    );
    let predicate = if complex_predicate {
        Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: vec![
                comparison,
                Expression::Constant(ConstantExpression::new(
                    Value::Boolean(true),
                    LogicalType::Boolean,
                )),
            ],
        })
    } else {
        comparison
    };
    bounded_group(Aggregate::new(
        1,
        2,
        3,
        values,
        vec![reference(0, LogicalType::Integer)],
        vec![],
        vec![source],
        vec![],
    ))
    .with_post_reduction(PostAggregateReduction {
        reduction_index: 4,
        reducers: vec![reducer],
        scalar_expressions: vec![scalar],
        predicate,
    })
}

fn lower_aggregate(
    ctx: &BindContext,
    aggregate: Aggregate,
    context: ExtractionContext,
    with_having: bool,
) -> crate::physical::specs::AggregateSpec {
    let aggregate_type = aggregate.aggregates[0].return_type();
    let aggregate = OwnedLogicalPlan::new(ctx, LogicalOperator::Aggregate(Box::new(aggregate)));
    let mut root = if with_having {
        OwnedLogicalPlan::new(
            ctx,
            LogicalOperator::Filter(Filter::new(
                aggregate,
                vec![comparison(
                    ComparisonType::GreaterThan,
                    reference(1, aggregate_type.clone()),
                    Expression::Constant(ConstantExpression::new(
                        match aggregate_type {
                            LogicalType::Decimal { precision, scale } => {
                                Value::Decimal(0, precision, scale)
                            }
                            _ => unreachable!(),
                        },
                        aggregate_type,
                    )),
                )],
            )),
        )
    } else {
        aggregate
    };
    crate::physical::slot_assignment::assign_expression_slots(&mut root.operator)
        .expect("resolve post-reduction domains");
    let plan = PhysicalPlanExtractor::new(context)
        .extract(&root)
        .expect("lower post-reduction aggregate");
    let aggregate_id = if matches!(plan.node(plan.root).kind, PhysicalNodeKind::Aggregate(_)) {
        plan.root
    } else {
        *plan
            .child_ids(&plan.node(plan.root).children)
            .first()
            .expect("filter/project has aggregate child")
    };
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(aggregate_id).kind else {
        panic!("expected aggregate node")
    };
    spec.as_ref().clone()
}

fn bounded_group(mut aggregate: Aggregate) -> Aggregate {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update_i32(&mut stats, 1);
    NumericStats::update_i32(&mut stats, 100);
    aggregate.group_stats[0] = Some(stats);
    aggregate
}

fn decimal_cast(child: Expression, target: LogicalType) -> Expression {
    let source = child.return_type();
    let mut casts = CastFunctionSet::new();
    casts.register_bind_function(bind_decimal_casts);
    let cast_info = casts.get_cast_function(&source, &target).unwrap();
    Expression::Cast(CastExpression::new(child, target, cast_info, false))
}

fn values(ctx: &BindContext, types: Vec<LogicalType>) -> OwnedLogicalPlan {
    OwnedLogicalPlan::new(
        ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            (0..types.len()).map(|index| format!("c{index}")).collect(),
            types,
        )),
    )
}

fn aggregate_column(index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(2, index), ty))
}

fn scalar_column(index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(4, index), ty))
}

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn comparison(kind: ComparisonType, left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpression::new(kind, left, right))
}

fn parallel_context() -> ExtractionContext {
    ExtractionContext {
        max_threads: 4,
        ..ExtractionContext::default()
    }
}
