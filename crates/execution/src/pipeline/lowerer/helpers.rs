// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_planner::operator::JoinCondition;

pub(crate) fn is_streaming_window_supported(spec: &WindowSpec) -> bool {
    spec.expressions.iter().all(|expr| {
        expr.function.function_type == WindowFunctionType::RowNumber
            && expr.children.is_empty()
            && expr.partitions.is_empty()
            && expr.orders.is_empty()
    })
}

pub(crate) fn ensure_streaming_topn_supported(spec: &TopNSpec) -> Result<()> {
    if spec.orders.is_empty() {
        return Err(paro_error::not_implemented(
            "TopN without ORDER BY should lower as Limit, not StreamingTopN",
        ));
    }
    debug_assert!(
        !spec.orders.is_empty(),
        "TopN without order should have lowered as streaming Limit"
    );
    Ok(())
}

pub(crate) fn ordering_spec_from_orders(
    orders: &[paro_planner::binder::ir::OrderByNode],
) -> OrderingSpec {
    let columns = orders
        .iter()
        .filter_map(|order| {
            let column = match &order.expression {
                Expression::Reference(reference) => reference.index,
                Expression::ColumnRef(column_ref) => column_ref.binding.column_index,
                _ => return None,
            };
            Some(OrderingColumn {
                column,
                direction: if order.ascending {
                    OrderingDirection::Asc
                } else {
                    OrderingDirection::Desc
                },
                nulls: if order.nulls_first {
                    NullOrdering::First
                } else {
                    NullOrdering::Last
                },
            })
        })
        .collect();
    OrderingSpec::new(columns)
}

pub(crate) fn needs_hash_join_unmatched_source(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
    )
}

pub(crate) fn hash_join_probe_transform(
    handle: BreakerHandleId,
    spec: &HashJoinSpec,
) -> TransformSpec {
    TransformSpec::HashJoinProbe(HashJoinProbeSpec {
        handle,
        join_type: spec.join_type,
        anti_join_mode: spec.anti_join_mode,
        conditions: all_hash_join_conditions(spec),
        left_projection: spec.left_projection.clone(),
        output_names: spec.output_names.clone(),
        output_types: spec.output_types.clone(),
        reduction_cascade: spec.reduction_cascade.clone(),
    })
}

pub(crate) fn all_hash_join_conditions(spec: &HashJoinSpec) -> Box<[JoinCondition]> {
    spec.key_conditions
        .iter()
        .chain(spec.residual_conditions.iter())
        .chain(
            spec.reduction_cascade
                .iter()
                .flat_map(|cascade| cascade.predicates.iter())
                .map(|predicate| &predicate.condition),
        )
        .cloned()
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

pub(crate) fn cross_product_probe_transform(
    handle: BreakerHandleId,
    spec: &CrossProductSpec,
) -> TransformSpec {
    TransformSpec::CrossProductProbe(CrossProductProbeSpec {
        handle,
        left_column_count: spec.left_output_types.len(),
        output_names: spec.output_names.clone(),
        output_types: spec.output_types.clone(),
    })
}

pub(crate) fn nlj_probe_transform(
    handle: BreakerHandleId,
    spec: &NestedLoopJoinSpec,
) -> TransformSpec {
    TransformSpec::NestedLoopJoinProbe(NestedLoopJoinProbeSpec {
        handle,
        join_type: spec.join_type,
        conditions: spec.conditions.clone(),
        mark_null_condition_start: spec.mark_null_condition_start,
        arbitrary_condition: spec.arbitrary_condition.clone(),
        left_projection: spec.left_projection.clone(),
        right_projection: spec.right_projection.clone(),
        right_output_types: spec.right_output_types.clone(),
        output_names: spec.output_names.clone(),
        output_types: spec.output_types.clone(),
    })
}

pub(crate) fn sort_range_probe_transform(
    handle: BreakerHandleId,
    spec: &SortRangeJoinSpec,
) -> TransformSpec {
    TransformSpec::SortRangeJoinProbe(SortRangeJoinProbeSpec {
        handle,
        join_type: spec.join_type,
        conditions: spec.conditions.clone(),
        mark_null_condition_start: spec.mark_null_condition_start,
        left_projection: spec.left_projection.clone(),
        right_projection: spec.right_projection.clone(),
        right_output_types: spec.right_output_types.clone(),
        output_names: spec.output_names.clone(),
        output_types: spec.output_types.clone(),
    })
}

pub(crate) fn needs_nlj_unmatched_source(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
    )
}

pub(crate) fn aggregate_build_sink_spec(handle: BreakerHandleId, spec: AggregateSpec) -> SinkSpec {
    if spec.grouping_key_count == 0 {
        return SinkSpec::UngroupedAggregate(UngroupedAggregateSinkSpec {
            handle,
            spec,
            required: Default::default(),
        });
    }
    if spec.perfect_hash.is_some() {
        return SinkSpec::PerfectHashAggregate(PerfectHashAggregateSinkSpec {
            handle,
            spec,
            required: Default::default(),
        });
    }
    SinkSpec::HashAggregateBuild(HashAggregateBuildSinkSpec {
        handle,
        spec,
        required: Default::default(),
    })
}

pub(crate) fn aggregate_emit_source_spec(
    handle: BreakerHandleId,
    spec: AggregateSpec,
) -> SourceSpec {
    if spec.grouping_key_count == 0 {
        return SourceSpec::UngroupedAggregateEmit(UngroupedAggregateEmitSourceSpec {
            handle,
            spec,
        });
    }
    if spec.perfect_hash.is_some() {
        return SourceSpec::PerfectHashAggregateEmit(PerfectHashAggregateEmitSourceSpec {
            handle,
            spec,
        });
    }
    SourceSpec::HashAggregateEmit(HashAggregateEmitSourceSpec { handle, spec })
}
