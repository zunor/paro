// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    FunctionExpression,
};
use paro_planner::operator::{
    CTERef, DependentJoin, ExpressionGet, Filter, Join, JoinType, LogicalOperator, MaterializedCTE,
    Projection, SetOperation,
};
use paro_planner::plan::LogicalPlan;

use super::{join_null_supplying_inputs, CTEPartitioner};
use crate::expression::traversal::visit_expression;

fn column(table: usize, ordinal: usize) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(
        paro_planner::operator::ColumnBinding::new(table, ordinal),
        LogicalType::Integer,
    ))
}

fn integer(value: i32) -> Expression {
    Expression::Constant(ConstantExpression::new(
        Value::Integer(value),
        LogicalType::Integer,
    ))
}

fn branch(bind_context: &BindContext, discriminator: i32) -> LogicalPlan {
    let source = bind_context.generate_table_index();
    let projection = bind_context.generate_table_index();
    let values = LogicalPlan::new(
        bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            source,
            vec![vec![integer(discriminator * 10)]],
            vec!["value".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    LogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(Projection::new(
            projection,
            values,
            vec![column(source, 0), integer(discriminator)],
        )),
    )
}

fn reference(
    bind_context: &BindContext,
    cte_index: usize,
    discriminator: i32,
    filtered: bool,
) -> LogicalPlan {
    reference_with_extra_filters(bind_context, cte_index, discriminator, filtered, Vec::new())
}

fn reference_with_extra_filters(
    bind_context: &BindContext,
    cte_index: usize,
    discriminator: i32,
    filtered: bool,
    mut extra_filters: Vec<Expression>,
) -> LogicalPlan {
    let table = bind_context.generate_table_index();
    let reference = LogicalPlan::new(
        bind_context,
        LogicalOperator::CTERef(CTERef::new(
            cte_index,
            table,
            "sales".to_string(),
            vec!["value".to_string(), "kind".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    if !filtered {
        return reference;
    }
    let mut filters = vec![Expression::Comparison(ComparisonExpression::new(
        ComparisonType::Equal,
        column(table, 1),
        integer(discriminator),
    ))];
    filters.append(&mut extra_filters);
    LogicalPlan::new(
        bind_context,
        LogicalOperator::Filter(Filter::new(reference, filters)),
    )
}

fn union(bind_context: &BindContext, left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    LogicalPlan::new(
        bind_context,
        LogicalOperator::SetOperation(SetOperation::union(
            bind_context.generate_table_index(),
            left,
            right,
            true,
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    )
}

fn plan_with_consumers(
    bind_context: &BindContext,
    build_consumers: impl FnOnce(usize) -> LogicalPlan,
) -> LogicalPlan {
    let cte_index = bind_context.generate_table_index();
    let producer = union(
        bind_context,
        union(
            bind_context,
            branch(bind_context, 1),
            branch(bind_context, 2),
        ),
        branch(bind_context, 3),
    );
    let consumers = build_consumers(cte_index);
    LogicalPlan::new(
        bind_context,
        LogicalOperator::MaterializedCTE(
            MaterializedCTE::new(
                cte_index,
                "sales".to_string(),
                vec!["value".to_string(), "kind".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
                CTEMaterialize::Default,
                producer,
                consumers,
            )
            .with_ref_count(3),
        ),
    )
}

fn plan(bind_context: &BindContext, leave_last_unfiltered: bool) -> LogicalPlan {
    plan_with_consumers(bind_context, |cte_index| {
        union(
            bind_context,
            union(
                bind_context,
                reference(bind_context, cte_index, 1, true),
                reference(bind_context, cte_index, 2, true),
            ),
            reference(bind_context, cte_index, 3, !leave_last_unfiltered),
        )
    })
}

fn join(
    bind_context: &BindContext,
    join_type: JoinType,
    left: LogicalPlan,
    right: LogicalPlan,
) -> LogicalPlan {
    LogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::any(
            join_type,
            left,
            right,
            Expression::Constant(ConstantExpression::new(
                Value::Boolean(true),
                LogicalType::Boolean,
            )),
        )),
    )
}

fn passthrough_projection(bind_context: &BindContext, child: LogicalPlan) -> LogicalPlan {
    let bindings = child.get_column_bindings();
    let types = child.types();
    LogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(Projection::new(
            bind_context.generate_table_index(),
            child,
            bindings
                .into_iter()
                .zip(types)
                .map(|(binding, return_type)| {
                    Expression::ColumnRef(ColumnRefExpression::new(binding, return_type))
                })
                .collect(),
        )),
    )
}

fn unrelated_rows(bind_context: &BindContext) -> LogicalPlan {
    LogicalPlan::new(
        bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            bind_context.generate_table_index(),
            vec![vec![integer(1)]],
            vec!["unrelated".to_string()],
            vec![LogicalType::Integer],
        )),
    )
}

fn volatile_predicate() -> Expression {
    let function = paro_function::scalar::math::get_random_function()
        .functions
        .into_iter()
        .next()
        .expect("random overload");
    Expression::Comparison(ComparisonExpression::new(
        ComparisonType::GreaterThan,
        Expression::Function(FunctionExpression::new(
            function,
            Vec::new(),
            LogicalType::Double,
        )),
        Expression::Constant(ConstantExpression::new(
            Value::Double(0.5),
            LogicalType::Double,
        )),
    ))
}

fn inspect_partition_owners(operator: &LogicalOperator, owners: &mut Vec<(bool, bool)>) {
    if let LogicalOperator::MaterializedCTE(cte) = operator {
        if cte.cte_name.contains("$partition") {
            owners.push((
                cte.materialized == CTEMaterialize::Materialized,
                matches!(cte.cte_query.operator, LogicalOperator::SetOperation(_)),
            ));
        }
    }
    for child in operator.children() {
        inspect_partition_owners(&child.operator, owners);
    }
}

fn count_partition_discriminator_references(plan: &mut LogicalPlan) -> usize {
    let mut reference_tables = Vec::new();
    plan.try_visit_pre_order(|node| {
        if let LogicalOperator::CTERef(reference) = &node.operator {
            reference_tables.push(reference.table_index);
        }
        Ok(())
    })
    .expect("collect partition references");
    fn count_in_operator(
        operator: &mut LogicalOperator,
        reference_tables: &[usize],
        count: &mut usize,
    ) {
        paro_planner::visitor::enumerate_expressions(operator, |expression| {
            visit_expression(expression, &mut |expression| {
                if let Expression::ColumnRef(column) = expression {
                    *count += usize::from(
                        column.depth == 0
                            && column.binding.column_index == 1
                            && reference_tables.contains(&column.binding.table_index),
                    );
                }
            });
        });
        let _ = operator.visit_children_mut(|child| {
            count_in_operator(&mut child.operator, reference_tables, count);
            std::ops::ControlFlow::Continue(())
        });
    }
    let mut count = 0usize;
    count_in_operator(&mut plan.operator, &reference_tables, &mut count);
    count
}

#[test]
fn publishes_complete_filtered_partition_materializations() {
    let bind_context = BindContext::new();
    let mut rewritten = CTEPartitioner::new(&bind_context)
        .optimize_default_root(plan(&bind_context, false))
        .expect("partitioning should match");

    let mut owners = Vec::new();
    inspect_partition_owners(&rewritten.operator, &mut owners);
    assert_eq!(owners.len(), 3, "{rewritten:#?}");
    assert!(owners.iter().all(|(materialized, _)| *materialized));
    assert!(owners.iter().all(|(_, contains_union)| !contains_union));
    assert_eq!(count_partition_discriminator_references(&mut rewritten), 0);
    assert!(CTEPartitioner::new(&bind_context)
        .optimize_default_root(rewritten)
        .is_none());
}

#[test]
fn rejects_partitioning_when_any_reference_lacks_a_local_filter() {
    let bind_context = BindContext::new();
    assert!(CTEPartitioner::new(&bind_context)
        .optimize_default_root(plan(&bind_context, true))
        .is_none());
}

#[test]
fn rejects_partitioning_when_a_producer_branch_has_no_reference() {
    let bind_context = BindContext::new();
    let candidate = plan(&bind_context, false);
    let (id, stats, operator) = candidate.into_parts();
    let LogicalOperator::MaterializedCTE(mut cte) = operator else {
        panic!("test candidate must be a materialized CTE");
    };
    cte.cte_query = Box::new(union(
        &bind_context,
        *cte.cte_query,
        branch(&bind_context, 4),
    ));
    let candidate = LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::MaterializedCTE(cte),
    };

    assert!(CTEPartitioner::new(&bind_context)
        .optimize_default_root(candidate)
        .is_none());
}

#[test]
fn null_supplying_join_input_contract_is_exhaustive() {
    assert_eq!(join_null_supplying_inputs(JoinType::Left), (false, true));
    assert_eq!(join_null_supplying_inputs(JoinType::Single), (false, true));
    assert_eq!(join_null_supplying_inputs(JoinType::Right), (true, false));
    assert_eq!(join_null_supplying_inputs(JoinType::Outer), (true, true));
    for join_type in [
        JoinType::Invalid,
        JoinType::Inner,
        JoinType::Semi,
        JoinType::Anti,
        JoinType::Mark,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ] {
        assert_eq!(join_null_supplying_inputs(join_type), (false, false));
    }
}

#[test]
fn rejects_reference_on_outer_join_null_supplying_side_even_behind_projection() {
    let bind_context = BindContext::new();
    let candidate = plan_with_consumers(&bind_context, |cte_index| {
        let preserved = reference(&bind_context, cte_index, 1, true);
        let null_supplying =
            passthrough_projection(&bind_context, reference(&bind_context, cte_index, 2, true));
        let outer = join(&bind_context, JoinType::Left, preserved, null_supplying);
        join(
            &bind_context,
            JoinType::Inner,
            outer,
            reference(&bind_context, cte_index, 3, true),
        )
    });

    assert!(
        CTEPartitioner::new(&bind_context)
            .optimize_default_root(candidate)
            .is_none(),
        "constant substitution must not depend on a projection shielding a null-extended binding"
    );
}

#[test]
fn permits_outer_join_when_every_reference_is_on_the_preserved_side() {
    let bind_context = BindContext::new();
    let candidate = plan_with_consumers(&bind_context, |cte_index| {
        let references = join(
            &bind_context,
            JoinType::Inner,
            join(
                &bind_context,
                JoinType::Inner,
                reference(&bind_context, cte_index, 1, true),
                reference(&bind_context, cte_index, 2, true),
            ),
            reference(&bind_context, cte_index, 3, true),
        );
        join(
            &bind_context,
            JoinType::Left,
            references,
            unrelated_rows(&bind_context),
        )
    });

    assert!(CTEPartitioner::new(&bind_context)
        .optimize_default_root(candidate)
        .is_some());
}

#[test]
fn rejects_reference_on_scalar_dependent_join_nullable_rhs() {
    let bind_context = BindContext::new();
    let candidate = plan_with_consumers(&bind_context, |cte_index| {
        let preserved = join(
            &bind_context,
            JoinType::Inner,
            reference(&bind_context, cte_index, 1, true),
            reference(&bind_context, cte_index, 3, true),
        );
        LogicalPlan::new(
            &bind_context,
            LogicalOperator::DependentJoin(DependentJoin::scalar(
                preserved,
                reference(&bind_context, cte_index, 2, true),
                Vec::new(),
                None,
            )),
        )
    });

    assert!(CTEPartitioner::new(&bind_context)
        .optimize_default_root(candidate)
        .is_none());
}

#[test]
fn rejects_an_incomplete_compound_recipe_without_an_error_channel() {
    let bind_context = BindContext::new();
    let candidate = plan_with_consumers(&bind_context, |cte_index| {
        union(
            &bind_context,
            union(
                &bind_context,
                reference_with_extra_filters(
                    &bind_context,
                    cte_index,
                    1,
                    true,
                    vec![volatile_predicate()],
                ),
                reference(&bind_context, cte_index, 2, true),
            ),
            reference(&bind_context, cte_index, 3, true),
        )
    });

    assert!(CTEPartitioner::new(&bind_context)
        .optimize_default_root(candidate)
        .is_none());
}
