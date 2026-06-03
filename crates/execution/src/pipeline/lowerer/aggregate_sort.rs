// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_aggregate_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &AggregateSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let child = self.only_child(root)?;
        let aggregate_output = self.plan.node(root).output.clone();
        let handle = self.handles.register(
            BreakerHandleKind::Aggregate,
            aggregate_output.clone(),
            Default::default(),
        );

        let producer_sink = aggregate_build_sink_spec(handle, spec.clone());
        let producer = self.lower_subtree_to_sink(
            child,
            producer_sink,
            SinkSharing::Exclusive,
            self.plan.node(child).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;

        let consumer_source = aggregate_emit_source_spec(handle, spec.clone());
        let pushed = self.push_pipeline(
            consumer_source,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.handles.add_consumer(handle, consumer)?;
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::FinalizeBeforeEmit,
        });
        Ok(pushed.tail)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_topn_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &TopNSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        ensure_streaming_topn_supported(spec)?;
        let child = self.only_child(root)?;
        let topn_output = self.plan.node(root).output.clone();
        let handle =
            self.handles
                .register(BreakerHandleKind::TopN, topn_output, Default::default());

        let producer = self.lower_subtree_to_sink(
            child,
            SinkSpec::TopNBuild(TopNBuildSinkSpec {
                handle,
                spec: spec.clone(),
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            self.plan.node(child).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;

        let consumer_source = SourceSpec::TopNEmit(TopNEmitSourceSpec {
            handle,
            spec: spec.clone(),
        });
        let pushed = self.push_pipeline(
            consumer_source,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.handles.add_consumer(handle, consumer)?;
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::FinalizeBeforeEmit,
        });
        Ok(pushed.tail)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_sort_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &SortSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let child = self.only_child(root)?;
        let sort_output = self.plan.node(root).output.clone();
        let input = self.plan.node(child).output.clone();
        let handle = self.handles.register(
            BreakerHandleKind::Sort,
            sort_output.clone(),
            Default::default(),
        );

        let producer = self.lower_subtree_to_sink(
            child,
            SinkSpec::SortBuild(SortBuildSinkSpec {
                handle,
                orders: spec.orders.clone(),
                projection_map: spec.projection_map.clone(),
                input_types: input.types.clone(),
                output_names: sort_output.names.clone(),
                output_types: sort_output.types.clone(),
                force_external: false,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            input,
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;

        let consumer_source = SourceSpec::SortEmit(SortEmitSourceSpec {
            handle,
            ordering: ordering_spec_from_orders(&spec.orders),
            output_names: sort_output.names.clone(),
            output_types: sort_output.types.clone(),
        });
        let pushed = self.push_pipeline(
            consumer_source,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.handles.add_consumer(handle, consumer)?;
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::FinalizeBeforeEmit,
        });
        Ok(pushed.tail)
    }

    pub(crate) fn collect_tail_to_breaker(
        &mut self,
        root: PhysicalPlanNodeId,
        mut is_breaker: impl FnMut(&PhysicalNodeKind) -> bool,
    ) -> Result<Option<BreakerTail>> {
        let mut current = root;
        let mut transforms = Vec::new();
        loop {
            let node = self.plan.node(current);
            match &node.kind {
                PhysicalNodeKind::Filter(spec) => {
                    transforms.push(TransformSpec::Filter(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Project(spec) => {
                    transforms.push(TransformSpec::Project(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Limit(spec) => {
                    transforms.push(TransformSpec::Limit(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Aggregate(spec) if is_streaming_aggregate_supported(spec) => {
                    let child = self.only_child(current)?;
                    if self.subtree_root_needs_post_join_fanout(child)? {
                        transforms.reverse();
                        return Ok(Some(BreakerTail {
                            breaker: current,
                            transforms,
                            output: self.plan.node(root).output.clone(),
                        }));
                    }
                    transforms.push(TransformSpec::StreamingAggregate(spec.clone()));
                    current = child;
                }
                PhysicalNodeKind::Window(spec) if is_streaming_window_supported(spec) => {
                    let child = self.only_child(current)?;
                    if self.subtree_root_needs_post_join_fanout(child)? {
                        transforms.reverse();
                        return Ok(Some(BreakerTail {
                            breaker: current,
                            transforms,
                            output: self.plan.node(root).output.clone(),
                        }));
                    }
                    transforms.push(TransformSpec::StreamingWindow(spec.clone()));
                    current = child;
                }
                PhysicalNodeKind::GraphExpand(spec) => {
                    transforms.push(TransformSpec::GraphExpand(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphProject(spec) => {
                    transforms.push(TransformSpec::GraphProject(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphShortestPath(spec) => {
                    transforms.push(TransformSpec::GraphShortestPath(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::ExternalProject(spec) => {
                    transforms.push(TransformSpec::ExternalProject(spec.clone()));
                    current = self.only_child(current)?;
                }
                kind if is_breaker(kind) && !transforms.is_empty() => {
                    transforms.reverse();
                    return Ok(Some(BreakerTail {
                        breaker: current,
                        transforms,
                        output: self.plan.node(root).output.clone(),
                    }));
                }
                _ => return Ok(None),
            }
        }
    }

    pub(crate) fn subtree_root_needs_post_join_fanout(
        &mut self,
        root: PhysicalPlanNodeId,
    ) -> Result<bool> {
        if let Some(result) = self.post_join_fanout_cache[root.index()] {
            return Ok(result);
        }

        let child = match &self.plan.node(root).kind {
            PhysicalNodeKind::HashJoin(_) => {
                let result = true;
                self.post_join_fanout_cache[root.index()] = Some(result);
                return Ok(result);
            }
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                let result = needs_nlj_unmatched_source(spec.join_type);
                self.post_join_fanout_cache[root.index()] = Some(result);
                return Ok(result);
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                let result = needs_nlj_unmatched_source(spec.join_type);
                self.post_join_fanout_cache[root.index()] = Some(result);
                return Ok(result);
            }
            PhysicalNodeKind::Filter(_)
            | PhysicalNodeKind::Project(_)
            | PhysicalNodeKind::Limit(_)
            | PhysicalNodeKind::GraphExpand(_)
            | PhysicalNodeKind::GraphProject(_)
            | PhysicalNodeKind::GraphShortestPath(_)
            | PhysicalNodeKind::ExternalProject(_) => Some(self.only_child(root)?),
            PhysicalNodeKind::Aggregate(spec) if is_streaming_aggregate_supported(spec) => {
                Some(self.only_child(root)?)
            }
            PhysicalNodeKind::Window(spec) if is_streaming_window_supported(spec) => {
                Some(self.only_child(root)?)
            }
            _ => None,
        };

        let result = match child {
            Some(child) => self.subtree_root_needs_post_join_fanout(child)?,
            None => false,
        };
        self.post_join_fanout_cache[root.index()] = Some(result);
        Ok(result)
    }
}
