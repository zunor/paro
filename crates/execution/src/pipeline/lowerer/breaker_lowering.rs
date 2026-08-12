// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Breaker-specific lowering dispatch.

use super::*;

pub(crate) enum BreakerDispatch {
    TopN(TopNSpec),
    Sort(SortSpec),
    Aggregate(AggregateSpec),
    SetOperation(SetOperationSpec),
    Window(WindowSpec),
    PartitionAggregateWindow(PartitionAggregateWindowSpec),
    HashJoin(HashJoinSpec),
    NestedLoopJoin(NestedLoopJoinSpec),
    SortRangeJoin(SortRangeJoinSpec),
    ClassicIeJoin(ClassicIeJoinSpec),
    CrossProduct(CrossProductSpec),
    ExternalTable(ExternalTableSpec),
}

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn breaker_dispatch_for_root(
        &self,
        root: PhysicalPlanNodeId,
    ) -> Option<BreakerDispatch> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::TopN(spec) => Some(BreakerDispatch::TopN(spec.clone())),
            PhysicalNodeKind::Sort(spec) => Some(BreakerDispatch::Sort(spec.clone())),
            PhysicalNodeKind::Aggregate(spec) => Some(BreakerDispatch::Aggregate(spec.clone())),
            PhysicalNodeKind::SetOperation(spec) => {
                Some(BreakerDispatch::SetOperation(spec.clone()))
            }
            PhysicalNodeKind::Window(spec) if !is_streaming_window_supported(spec) => {
                Some(BreakerDispatch::Window(spec.clone()))
            }
            PhysicalNodeKind::PartitionAggregateWindow(spec) => {
                Some(BreakerDispatch::PartitionAggregateWindow(spec.clone()))
            }
            PhysicalNodeKind::HashJoin(spec) => Some(BreakerDispatch::HashJoin(spec.clone())),
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                Some(BreakerDispatch::NestedLoopJoin(spec.clone()))
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                Some(BreakerDispatch::SortRangeJoin(spec.clone()))
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                Some(BreakerDispatch::ClassicIeJoin(spec.clone()))
            }
            PhysicalNodeKind::CrossProduct(spec) => {
                Some(BreakerDispatch::CrossProduct(spec.clone()))
            }
            PhysicalNodeKind::ExternalTable(spec) => {
                Some(BreakerDispatch::ExternalTable(spec.clone()))
            }
            _ => None,
        }
    }

    pub(crate) fn tail_breaker_dispatch(
        &self,
        root: PhysicalPlanNodeId,
    ) -> Result<BreakerDispatch> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::TopN(spec) => Ok(BreakerDispatch::TopN(spec.clone())),
            PhysicalNodeKind::Sort(spec) => Ok(BreakerDispatch::Sort(spec.clone())),
            PhysicalNodeKind::Aggregate(spec) => Ok(BreakerDispatch::Aggregate(spec.clone())),
            PhysicalNodeKind::SetOperation(spec) => Ok(BreakerDispatch::SetOperation(spec.clone())),
            PhysicalNodeKind::Window(spec) => Ok(BreakerDispatch::Window(spec.clone())),
            PhysicalNodeKind::PartitionAggregateWindow(spec) => {
                Ok(BreakerDispatch::PartitionAggregateWindow(spec.clone()))
            }
            PhysicalNodeKind::HashJoin(spec) => Ok(BreakerDispatch::HashJoin(spec.clone())),
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                Ok(BreakerDispatch::NestedLoopJoin(spec.clone()))
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                Ok(BreakerDispatch::SortRangeJoin(spec.clone()))
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                Ok(BreakerDispatch::ClassicIeJoin(spec.clone()))
            }
            PhysicalNodeKind::CrossProduct(spec) => Ok(BreakerDispatch::CrossProduct(spec.clone())),
            PhysicalNodeKind::ExternalTable(spec) => {
                Ok(BreakerDispatch::ExternalTable(spec.clone()))
            }
            _ => Err(paro_error::internal(
                "collect_tail_to_breaker returned a non-breaker node",
            )),
        }
    }

    pub(crate) fn is_tail_breaker(kind: &PhysicalNodeKind) -> bool {
        match kind {
            PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::Sort(_)
            | PhysicalNodeKind::HashJoin(_)
            | PhysicalNodeKind::NestedLoopJoin(_)
            | PhysicalNodeKind::SortRangeJoin(_)
            | PhysicalNodeKind::ClassicIeJoin(_)
            | PhysicalNodeKind::CrossProduct(_)
            | PhysicalNodeKind::ExternalTable(_)
            | PhysicalNodeKind::SetOperation(_)
            | PhysicalNodeKind::DelimJoin(_)
            | PhysicalNodeKind::RecursiveCte(_) => true,
            PhysicalNodeKind::Aggregate(_) => true,
            PhysicalNodeKind::Window(spec) => !is_streaming_window_supported(spec),
            PhysicalNodeKind::PartitionAggregateWindow(_) => true,
            _ => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dispatch_breaker_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        breaker: BreakerDispatch,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        match breaker {
            BreakerDispatch::TopN(spec) => self.lower_topn_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::Sort(spec) => self.lower_sort_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::Aggregate(spec) => self.lower_aggregate_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::SetOperation(spec) => self.lower_set_operation_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::Window(spec) => self.lower_window_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::PartitionAggregateWindow(spec) => self
                .lower_partition_aggregate_window_to_sink(
                    root,
                    &spec,
                    transforms,
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                ),
            BreakerDispatch::HashJoin(spec) => self.lower_hash_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::NestedLoopJoin(spec) => self.lower_nested_loop_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::SortRangeJoin(spec) => self.lower_sort_range_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::ClassicIeJoin(spec) => self.lower_classic_ie_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::CrossProduct(spec) => self.lower_cross_product_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::ExternalTable(spec) => self.lower_external_table_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
        }
    }
}
