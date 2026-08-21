// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_pipeline(
        &mut self,
        source: SourceSpec,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> Result<PipelineChain> {
        let properties = self.build_pipeline_properties(&source, &transforms, &sink);
        let id = self.push_pipeline_stage(
            source,
            transforms,
            sink,
            sink_sharing,
            output,
            properties,
            pipelines,
        );
        Ok(PipelineChain {
            entry: id,
            tail: id,
        })
    }

    fn build_pipeline_properties(
        &self,
        source: &SourceSpec,
        transforms: &[TransformSpec],
        sink: &SinkSpec,
    ) -> crate::physical::properties::PipelineProperties {
        let mut accumulator = PipelinePropertyAccumulator::start_from_source(&source);
        for transform in transforms {
            accumulator.apply_transform(transform);
        }
        accumulator.close_with_sink(sink)
    }

    #[allow(clippy::too_many_arguments)]
    fn push_pipeline_stage(
        &self,
        source: SourceSpec,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        properties: crate::physical::properties::PipelineProperties,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> PipelineId {
        let id = PipelineId::new(pipelines.len());
        pipelines.push(PipelineSpec {
            id,
            source,
            transforms,
            sink,
            sink_sharing,
            properties,
            output,
        });
        id
    }

    pub(crate) fn add_source_handle_dependencies(
        &mut self,
        source: &SourceSpec,
        consumer: PipelineId,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<()> {
        if let SourceSpec::CteScan(source) = source {
            self.handles.add_consumer(source.handle, consumer)?;
            let producer = self
                .cte_producers
                .get(&source.handle)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal("CTE scan source has no materialize producer")
                })?;
            dependencies.push(PipelineDependency {
                producer,
                consumer,
                kind: DependencyKind::MaterializeBeforeRead,
            });
        } else if let SourceSpec::DelimScan(source) = source {
            self.handles.add_consumer(source.handle, consumer)?;
        } else if let SourceSpec::RecursiveTableScan(source) = source {
            self.handles.add_consumer(source.handle, consumer)?;
        }
        Ok(())
    }

    pub(crate) fn lower_terminal_sink(
        &mut self,
        child: PhysicalPlanNodeId,
        sink: SinkSpec,
        output: RowType,
    ) -> Result<PipelineGraph> {
        let mut pipelines = Vec::new();
        let mut dependencies = Vec::new();
        let root_pipeline = self.lower_subtree_to_sink(
            child,
            sink,
            SinkSharing::Exclusive,
            output,
            &mut pipelines,
            &mut dependencies,
        )?;
        let root = self.pipeline_root_for(root_pipeline)?;
        let graph = PipelineGraph {
            pipelines,
            dependencies,
            handles: mem::take(&mut self.handles).finish(),
            control_regions: mem::take(&mut self.control_regions),
            root,
        };
        graph.validate()?;
        Ok(graph)
    }
}
