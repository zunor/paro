use std::sync::Arc;

use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry};
use paro_common::runtime_value::Value;
use paro_function::aggregate::distributive::count::{get_count_function, get_count_star_function};
use paro_function::aggregate::distributive::first_last::get_first_function;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::scalar::cast::BoundCastInfo;
use paro_planner::expression::{
    CastExpression, ComparisonExpression, ComparisonType, ConstantExpression, OperatorExpression,
};
use paro_planner::operator::{Aggregate, CrossProduct, Filter, Get, Projection};
use paro_storage::table::table_factory::TableFactory;

use super::*;

const GROUPED_SOURCE: usize = 10;
const SCALAR_SOURCE: usize = 20;
const GROUP_INDEX: usize = 30;
const GROUP_AGGREGATE: usize = 31;
const GROUPINGS_INDEX: usize = 32;
const SCALAR_GROUP: usize = 40;
const SCALAR_AGGREGATE: usize = 41;
const SCALAR_GROUPINGS: usize = 42;
const SCALAR_PROJECTION: usize = 43;
const WRAPPER_GROUP: usize = 50;
const WRAPPER_AGGREGATE: usize = 51;
const WRAPPER_GROUPINGS: usize = 52;
const WRAPPER_PROJECTION: usize = 53;
const OUTPUT_PROJECTION: usize = 60;

fn column(table: usize, index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(table, index),
        ty,
    ))
}

fn sum(input: Expression) -> Expression {
    let input_type = input.return_type();
    let (function, targets) = get_sum_function()
        .bind(std::slice::from_ref(&input_type))
        .expect("bind SUM");
    assert_eq!(targets, [input_type]);
    let return_type = function.return_type.clone();
    Expression::Aggregate(AggregateExpression::new(function, vec![input], return_type))
}

fn table(object_id: u64) -> Arc<TableCatalogEntry> {
    let types = vec![LogicalType::BigInt, LogicalType::Integer];
    let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
    let info = CreateTableInfo::new(
        "paro".to_string(),
        "public".to_string(),
        format!("source_{object_id}"),
        vec![
            ColumnDefinition::new("key".to_string(), types[0].clone()),
            ColumnDefinition::new("value".to_string(), types[1].clone()),
        ],
    );
    Arc::new(
        TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(object_id), 0)
            .unwrap(),
    )
}

fn get(table_index: usize, table: Arc<TableCatalogEntry>) -> LogicalPlan {
    LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
        table_index,
        vec!["key".to_string(), "value".to_string()],
        vec![LogicalType::BigInt, LogicalType::Integer],
        table,
    )))
}

fn q11_shape(
    grouped_table: Arc<TableCatalogEntry>,
    scalar_table: Arc<TableCatalogEntry>,
) -> LogicalPlan {
    let grouped = Aggregate::new(
        GROUP_INDEX,
        GROUP_AGGREGATE,
        GROUPINGS_INDEX,
        get(GROUPED_SOURCE, grouped_table),
        vec![column(GROUPED_SOURCE, 0, LogicalType::BigInt)],
        vec![],
        vec![sum(column(GROUPED_SOURCE, 1, LogicalType::Integer))],
        vec![],
    );

    let scalar_aggregate = Aggregate::new(
        SCALAR_GROUP,
        SCALAR_AGGREGATE,
        SCALAR_GROUPINGS,
        get(SCALAR_SOURCE, scalar_table),
        vec![],
        vec![],
        vec![sum(column(SCALAR_SOURCE, 1, LogicalType::Integer))],
        vec![],
    );
    let scalar_expression = Expression::Operator(OperatorExpression::new(
        OperatorType::Coalesce,
        vec![
            column(SCALAR_AGGREGATE, 0, LogicalType::BigInt),
            Expression::Constant(ConstantExpression::new(
                Value::BigInt(0),
                LogicalType::BigInt,
            )),
        ],
        LogicalType::BigInt,
    ));
    let scalar_projection = Projection::new(
        SCALAR_PROJECTION,
        LogicalPlan::synthetic(LogicalOperator::Aggregate(scalar_aggregate)),
        vec![scalar_expression],
    );

    let (first, _) = get_first_function()
        .bind(&[LogicalType::BigInt])
        .expect("bind FIRST");
    let wrapper = Aggregate::new(
        WRAPPER_GROUP,
        WRAPPER_AGGREGATE,
        WRAPPER_GROUPINGS,
        LogicalPlan::synthetic(LogicalOperator::Projection(scalar_projection)),
        vec![],
        vec![],
        vec![
            Expression::Aggregate(AggregateExpression::new(
                first,
                vec![column(SCALAR_PROJECTION, 0, LogicalType::BigInt)],
                LogicalType::BigInt,
            )),
            Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                vec![],
                LogicalType::BigInt,
            )),
        ],
        vec![],
    );
    let checked = Expression::Operator(OperatorExpression::new(
        OperatorType::ErrorIfMultipleRows,
        vec![
            column(WRAPPER_AGGREGATE, 0, LogicalType::BigInt),
            column(WRAPPER_AGGREGATE, 1, LogicalType::BigInt),
        ],
        LogicalType::BigInt,
    ));
    let wrapper_projection = Projection::new(
        WRAPPER_PROJECTION,
        LogicalPlan::synthetic(LogicalOperator::Aggregate(wrapper)),
        vec![checked],
    );
    let cross = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        LogicalPlan::synthetic(LogicalOperator::Aggregate(grouped)),
        LogicalPlan::synthetic(LogicalOperator::Projection(wrapper_projection)),
    ))));
    let predicate = Expression::Comparison(ComparisonExpression::new(
        ComparisonType::GreaterThan,
        column(GROUP_AGGREGATE, 0, LogicalType::BigInt),
        column(WRAPPER_PROJECTION, 0, LogicalType::BigInt),
    ));
    let filter =
        LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(cross, vec![predicate])));
    LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        OUTPUT_PROJECTION,
        filter,
        vec![
            column(GROUP_INDEX, 0, LogicalType::BigInt),
            column(GROUP_AGGREGATE, 0, LogicalType::BigInt),
        ],
    )))
}

#[test]
fn folds_alpha_equivalent_scalar_sum_into_grouped_reduction() {
    let source = table(91_001);
    let context = BindContext::new();
    let optimized = optimize_plan(q11_shape(source.clone(), source), &context);

    let LogicalOperator::Projection(output) = &optimized.operator else {
        panic!("expected the output projection");
    };
    let LogicalOperator::Aggregate(grouped) = &output.child.operator else {
        panic!("expected the grouped aggregate to replace Filter/CrossProduct");
    };
    let reduction = grouped
        .post_reduction
        .as_ref()
        .expect("post-aggregate reduction");
    assert_eq!(reduction.reducers.len(), 1);
    let Expression::Aggregate(reducer) = &reduction.reducers[0] else {
        panic!("expected aggregate reducer");
    };
    assert_eq!(reducer.function.name, "sum_partial_merge");
    assert_eq!(reducer.function.arguments, [LogicalType::BigInt]);
    assert_eq!(reducer.return_type, LogicalType::BigInt);

    let Expression::Operator(scalar) = &reduction.scalar_expressions[0] else {
        panic!("bound scalar projection must be preserved");
    };
    assert_eq!(scalar.operator_type, OperatorType::Coalesce);
    assert!(matches!(
        scalar.children.first(),
        Some(Expression::Reference(reference))
            if reference.index == 0 && reference.return_type == LogicalType::BigInt
    ));

    let Expression::Comparison(predicate) = &reduction.predicate else {
        panic!("HAVING comparison must be preserved");
    };
    assert_eq!(predicate.comparison_type, ComparisonType::GreaterThan);
    assert!(matches!(
        predicate.right.as_ref(),
        Expression::ColumnRef(column)
            if column.binding == ColumnBinding::new(reduction.reduction_index, 0)
    ));
    grouped
        .verify_post_reduction()
        .expect("valid reduction domains");
}

#[test]
fn refuses_sources_with_different_stable_table_identity() {
    let context = BindContext::new();
    let optimized = optimize_plan(q11_shape(table(91_002), table(91_003)), &context);

    assert!(matches!(
        optimized.operator,
        LogicalOperator::Projection(Projection { child, .. })
            if matches!(child.operator, LogicalOperator::Filter(_))
    ));
}

#[test]
fn refuses_grouping_sets_even_when_the_sources_match() {
    let source = table(91_004);
    let mut plan = q11_shape(source.clone(), source);
    let LogicalOperator::Projection(output) = &mut plan.operator else {
        panic!("output projection");
    };
    let LogicalOperator::Filter(filter) = &mut output.child.operator else {
        panic!("filter");
    };
    let LogicalOperator::Join(Join::Cross(cross)) = &mut filter.child.operator else {
        panic!("cross");
    };
    let LogicalOperator::Aggregate(grouped) = &mut cross.left.operator else {
        panic!("grouped aggregate");
    };
    grouped.grouping_sets = vec![paro_planner::binder::ir::GroupingSet {
        expressions: vec![0],
    }];

    let optimized = optimize_plan(plan, &BindContext::new());
    assert!(matches!(
        optimized.operator,
        LogicalOperator::Projection(Projection { child, .. })
            if matches!(child.operator, LogicalOperator::Filter(_))
    ));
}

#[test]
fn source_equivalence_maps_compacted_ordinals_by_physical_column_id() {
    let source = table(91_005);
    let grouped = get(GROUPED_SOURCE, source.clone());
    let mut scalar = get(SCALAR_SOURCE, source);
    let LogicalOperator::Get(scalar_get) = &mut scalar.operator else {
        panic!("scalar Get");
    };
    scalar_get.column_ids = vec![1];
    scalar_get.column_projections = vec![paro_planner::operator::GetColumnProjection::Stored];
    scalar_get.column_types = vec![LogicalType::Integer];
    scalar_get.returned_types = vec![LogicalType::Integer];
    scalar_get.names = vec!["value".to_string()];

    let bindings = AlphaBindings::match_sources(&grouped, &scalar)
        .expect("the scalar source is a semantic subset of the grouped source");
    assert!(bindings.expressions_equal(
        &column(GROUPED_SOURCE, 1, LogicalType::Integer),
        &column(SCALAR_SOURCE, 0, LogicalType::Integer),
    ));
    assert!(!bindings.expressions_equal(
        &column(GROUPED_SOURCE, 0, LogicalType::BigInt),
        &column(SCALAR_SOURCE, 0, LogicalType::Integer),
    ));
}

#[test]
fn source_equivalence_distinguishes_cast_from_try_cast() {
    let mut bindings = AlphaBindings::default();
    assert!(bindings.bind(
        ColumnBinding::new(GROUPED_SOURCE, 0),
        ColumnBinding::new(SCALAR_SOURCE, 0),
    ));
    let grouped = Expression::Cast(CastExpression::new(
        column(GROUPED_SOURCE, 0, LogicalType::BigInt),
        LogicalType::BigInt,
        BoundCastInfo::identity(&LogicalType::BigInt, &LogicalType::BigInt),
        false,
    ));
    let scalar = Expression::Cast(CastExpression::new(
        column(SCALAR_SOURCE, 0, LogicalType::BigInt),
        LogicalType::BigInt,
        BoundCastInfo::identity(&LogicalType::BigInt, &LogicalType::BigInt),
        true,
    ));

    assert!(!bindings.expressions_equal(&grouped, &scalar));
}

#[test]
fn same_display_signature_with_a_different_kernel_is_not_reused() {
    let source = table(91_006);
    let mut plan = q11_shape(source.clone(), source);
    let LogicalOperator::Projection(output) = &mut plan.operator else {
        panic!("output projection");
    };
    let LogicalOperator::Filter(filter) = &mut output.child.operator else {
        panic!("filter");
    };
    let LogicalOperator::Join(Join::Cross(cross)) = &mut filter.child.operator else {
        panic!("cross");
    };
    let LogicalOperator::Projection(wrapper_projection) = &mut cross.right.operator else {
        panic!("wrapper projection");
    };
    let LogicalOperator::Aggregate(wrapper) = &mut wrapper_projection.child.operator else {
        panic!("wrapper aggregate");
    };
    let LogicalOperator::Projection(scalar_projection) = &mut wrapper.child.operator else {
        panic!("scalar projection");
    };
    let LogicalOperator::Aggregate(scalar) = &mut scalar_projection.child.operator else {
        panic!("scalar aggregate");
    };
    let Expression::Aggregate(scalar_sum) = &mut scalar.aggregates[0] else {
        panic!("scalar SUM");
    };
    let (mut impostor, _) = get_count_function()
        .bind(&[LogicalType::Integer])
        .expect("bind COUNT");
    impostor.name = "sum".to_string();
    impostor.algebra = Some(AggregateAlgebra::Sum);
    scalar_sum.function = impostor;

    let optimized = optimize_plan(plan, &BindContext::new());
    assert!(matches!(
        optimized.operator,
        LogicalOperator::Projection(Projection { child, .. })
            if matches!(child.operator, LogicalOperator::Filter(_))
    ));
}

#[test]
fn scalar_wrapper_output_that_escapes_the_boundary_prevents_rewrite() {
    let source = table(91_007);
    let mut plan = q11_shape(source.clone(), source);
    let LogicalOperator::Projection(output) = &mut plan.operator else {
        panic!("output projection");
    };
    output
        .expressions
        .push(column(WRAPPER_PROJECTION, 0, LogicalType::BigInt));
    output.returned_types.push(LogicalType::BigInt);
    output.output_names.push("scalar_value".to_string());

    let optimized = optimize_plan(plan, &BindContext::new());
    let LogicalOperator::Projection(output) = &optimized.operator else {
        panic!("output projection");
    };
    assert!(matches!(output.child.operator, LogicalOperator::Filter(_)));
}
