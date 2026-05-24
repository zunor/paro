// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn lower_subtree_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::MaterializedCte(spec) => {
                let spec = spec.clone();
                return self.lower_materialized_cte_to_sink(
                    root,
                    spec,
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                );
            }
            PhysicalNodeKind::RecursiveCte(spec) => {
                let spec = spec.clone();
                return self.lower_recursive_cte_to_sink(
                    root,
                    spec,
                    Vec::new(),
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                );
            }
            PhysicalNodeKind::DelimJoin(spec) => {
                let spec = spec.clone();
                return self.lower_delim_join_to_sink(
                    root,
                    spec,
                    Vec::new(),
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                );
            }
            _ => {}
        }

        let breaker = match &self.plan.node(root).kind {
            PhysicalNodeKind::TopN(spec) => Some(BreakerDispatch::TopN(spec.clone())),
            PhysicalNodeKind::Sort(spec) => Some(BreakerDispatch::Sort(spec.clone())),
            PhysicalNodeKind::Aggregate(spec) if !is_streaming_aggregate_supported(spec) => {
                Some(BreakerDispatch::Aggregate(spec.clone()))
            }
            PhysicalNodeKind::SetOperation(spec) => {
                Some(BreakerDispatch::SetOperation(spec.clone()))
            }
            PhysicalNodeKind::Window(spec) if !is_streaming_window_supported(spec) => {
                Some(BreakerDispatch::Window(spec.clone()))
            }
            PhysicalNodeKind::HashJoin(spec) => Some(BreakerDispatch::HashJoin(spec.clone())),
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                Some(BreakerDispatch::NestedLoopJoin(spec.clone()))
            }
            PhysicalNodeKind::IEJoin(spec) => Some(BreakerDispatch::IEJoin(spec.clone())),
            PhysicalNodeKind::CrossProduct(spec) => {
                Some(BreakerDispatch::CrossProduct(spec.clone()))
            }
            PhysicalNodeKind::ExternalTable(spec) => {
                Some(BreakerDispatch::ExternalTable(spec.clone()))
            }
            _ => None,
        };
        if let Some(breaker) = breaker {
            return self.dispatch_breaker_to_sink(
                root,
                breaker,
                Vec::new(),
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            );
        }

        if let Some(tail) = self.collect_tail_to_breaker(root, |kind| match kind {
            PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::Sort(_)
            | PhysicalNodeKind::HashJoin(_)
            | PhysicalNodeKind::NestedLoopJoin(_)
            | PhysicalNodeKind::IEJoin(_)
            | PhysicalNodeKind::CrossProduct(_)
            | PhysicalNodeKind::ExternalTable(_)
            | PhysicalNodeKind::SetOperation(_)
            | PhysicalNodeKind::DelimJoin(_)
            | PhysicalNodeKind::RecursiveCte(_) => true,
            PhysicalNodeKind::Aggregate(spec) => !is_streaming_aggregate_supported(spec),
            PhysicalNodeKind::Window(spec) => !is_streaming_window_supported(spec),
            _ => false,
        })? {
            match &self.plan.node(tail.breaker).kind {
                PhysicalNodeKind::RecursiveCte(spec) => {
                    let spec = spec.clone();
                    return self.lower_recursive_cte_to_sink(
                        tail.breaker,
                        spec,
                        tail.transforms,
                        sink,
                        sink_sharing,
                        tail.output,
                        pipelines,
                        dependencies,
                    );
                }
                PhysicalNodeKind::DelimJoin(spec) => {
                    let spec = spec.clone();
                    return self.lower_delim_join_to_sink(
                        tail.breaker,
                        spec,
                        tail.transforms,
                        sink,
                        sink_sharing,
                        tail.output,
                        pipelines,
                        dependencies,
                    );
                }
                _ => {}
            }
            let breaker = match &self.plan.node(tail.breaker).kind {
                PhysicalNodeKind::TopN(spec) => BreakerDispatch::TopN(spec.clone()),
                PhysicalNodeKind::Sort(spec) => BreakerDispatch::Sort(spec.clone()),
                PhysicalNodeKind::Aggregate(spec) => BreakerDispatch::Aggregate(spec.clone()),
                PhysicalNodeKind::SetOperation(spec) => BreakerDispatch::SetOperation(spec.clone()),
                PhysicalNodeKind::Window(spec) => BreakerDispatch::Window(spec.clone()),
                PhysicalNodeKind::HashJoin(spec) => BreakerDispatch::HashJoin(spec.clone()),
                PhysicalNodeKind::NestedLoopJoin(spec) => {
                    BreakerDispatch::NestedLoopJoin(spec.clone())
                }
                PhysicalNodeKind::IEJoin(spec) => BreakerDispatch::IEJoin(spec.clone()),
                PhysicalNodeKind::CrossProduct(spec) => BreakerDispatch::CrossProduct(spec.clone()),
                PhysicalNodeKind::ExternalTable(spec) => {
                    BreakerDispatch::ExternalTable(spec.clone())
                }
                _ => unreachable!("collect_tail_to_breaker only returns known breaker kinds"),
            };
            return self.dispatch_breaker_to_sink(
                tail.breaker,
                breaker,
                tail.transforms,
                sink,
                sink_sharing,
                tail.output,
                pipelines,
                dependencies,
            );
        }

        self.lower_linear_pipeline_to_sink(
            root,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )
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
            // TODO(ORR4.3): correctness-first fallback. IE join is a breaker in the
            // typed runtime, but the sort-based IE strategy and EXPLAIN nl_fallback
            // marker are tracked separately from this R1 recovery.
            BreakerDispatch::IEJoin(spec) => self.lower_nested_loop_join_to_sink(
                root,
                &NestedLoopJoinSpec {
                    join_type: spec.join_type,
                    conditions: spec.conditions,
                    mark_null_condition_start: spec.mark_null_condition_start,
                    arbitrary_condition: None,
                    left_projection: spec.left_projection,
                    right_projection: spec.right_projection,
                    left_output_types: spec.left_output_types,
                    right_output_types: spec.right_output_types,
                    output_names: spec.output_names,
                    output_types: spec.output_types,
                },
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

    pub(crate) fn lower_linear_pipeline(
        &mut self,
        root: PhysicalPlanNodeId,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> Result<PipelineId> {
        let output = self.plan.node(root).output.clone();
        let (source, transforms) = self.collect_linear_roles(root)?;
        let mut dependencies = Vec::new();
        Ok(self
            .push_pipeline(
                source,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                &mut dependencies,
            )?
            .tail)
    }

    pub(crate) fn lower_linear_pipeline_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let (source, transforms) = self.collect_linear_roles(root)?;
        let source_handles = source.clone();
        let pushed = self.push_pipeline(
            source,
            transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        self.add_source_handle_dependencies(&source_handles, pushed.entry, dependencies)?;
        Ok(pushed.tail)
    }

    pub(crate) fn next_shared_sink(&mut self) -> SharedSinkId {
        let id = SharedSinkId::new(self.next_shared_sink);
        self.next_shared_sink += 1;
        id
    }

    pub(crate) fn pipeline_root_for(&self, pipeline: PipelineId) -> Result<PipelineRoot> {
        if let Some(region) = self.control_region_roots.get(&pipeline).copied() {
            return Ok(PipelineRoot::ControlRegion(region));
        }
        Ok(PipelineRoot::Pipeline(pipeline))
    }

    pub(crate) fn dependent_subgraph_roots(
        &self,
        first_pipeline: usize,
        end_pipeline: usize,
        join: PipelineId,
    ) -> Result<Vec<PipelineSubgraphRoot>> {
        if self.control_region_roots.contains_key(&join) {
            return Err(paro_error::internal(
                "correlated subquery join pipeline cannot itself be a nested control region",
            ));
        }

        let mut region_roots = self
            .control_region_roots
            .iter()
            .filter_map(|(pipeline, region)| {
                let idx = pipeline.index();
                (idx >= first_pipeline && idx < end_pipeline).then_some((*pipeline, *region))
            })
            .collect::<Vec<_>>();
        region_roots.sort_unstable_by_key(|(pipeline, _)| pipeline.index());

        let mut skip = vec![false; end_pipeline.saturating_sub(first_pipeline)];
        for (_, region) in &region_roots {
            self.mark_control_region_members(*region, first_pipeline, &mut skip)?;
        }

        let mut roots = Vec::new();
        for idx in first_pipeline..end_pipeline {
            let pipeline = PipelineId::new(idx);
            if pipeline == join {
                continue;
            }
            if let Some((_, region)) = region_roots
                .iter()
                .find(|(root_pipeline, _)| *root_pipeline == pipeline)
            {
                roots.push(PipelineSubgraphRoot::ControlRegion(*region));
                continue;
            }
            if skip[idx - first_pipeline] {
                continue;
            }
            roots.push(PipelineSubgraphRoot::Pipeline(pipeline));
        }
        Ok(roots)
    }

    pub(crate) fn mark_control_region_members(
        &self,
        region: ControlRegionId,
        first_pipeline: usize,
        skip: &mut [bool],
    ) -> Result<()> {
        let Some(region) = self.control_regions.get(region.index()) else {
            return Err(paro_error::internal(
                "correlated subquery references invalid nested control region",
            ));
        };

        match region {
            ControlRegion::RecursiveCte(region) => {
                self.mark_pipeline_in_range(region.anchor, first_pipeline, skip);
                for pipeline in &region.recursive {
                    self.mark_pipeline_in_range(*pipeline, first_pipeline, skip);
                }
                self.mark_pipeline_in_range(region.emit, first_pipeline, skip);
            }
            ControlRegion::CorrelatedSubquery(region) => {
                self.mark_pipeline_in_range(region.capture, first_pipeline, skip);
                for root in &region.dependent_roots {
                    match root {
                        PipelineSubgraphRoot::Pipeline(pipeline) => {
                            self.mark_pipeline_in_range(*pipeline, first_pipeline, skip);
                        }
                        PipelineSubgraphRoot::ControlRegion(region) => {
                            self.mark_control_region_members(*region, first_pipeline, skip)?;
                        }
                    }
                }
                self.mark_pipeline_in_range(region.join, first_pipeline, skip);
            }
        }
        Ok(())
    }

    pub(crate) fn mark_pipeline_in_range(
        &self,
        pipeline: PipelineId,
        first_pipeline: usize,
        skip: &mut [bool],
    ) {
        if let Some(offset) = pipeline.index().checked_sub(first_pipeline) {
            if let Some(slot) = skip.get_mut(offset) {
                *slot = true;
            }
        }
    }
}
