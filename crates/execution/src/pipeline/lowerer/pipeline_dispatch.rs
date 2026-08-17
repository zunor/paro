// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline-level dispatch helpers that are not breaker-specific.

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn lower_linear_pipeline(
        &mut self,
        root: PhysicalPlanNodeId,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> Result<PipelineId> {
        let output = self.plan.node(root).output.clone();
        let (source, transforms) = self.collect_linear_roles(root)?;
        Ok(self
            .push_pipeline(source, transforms, sink, sink_sharing, output, pipelines)?
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
        let pushed =
            self.push_pipeline(source, transforms, sink, sink_sharing, output, pipelines)?;
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
