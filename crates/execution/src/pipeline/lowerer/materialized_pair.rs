// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub fn lower_materialized_pair(
        &mut self,
        producer_root: PhysicalPlanNodeId,
        consumer_source: SourceSpec,
        row_type: RowType,
    ) -> Result<PipelineGraph> {
        use crate::physical::properties::PipelineProperties;
        use crate::pipeline::graph::{DependencyKind, MaterializeSinkSpec, PipelineDependency};
        use crate::pipeline::handles::BreakerHandleKind;

        let mut pipelines = Vec::new();
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            row_type.clone(),
            PipelineProperties::default(),
        );
        let producer = self.lower_linear_pipeline(
            producer_root,
            SinkSpec::Materialize(MaterializeSinkSpec { handle }),
            SinkSharing::Exclusive,
            &mut pipelines,
        )?;

        self.handles.set_producer(handle, producer)?;

        let consumer_sink = SinkSpec::ClientResult(ClientResultSpec::default());
        let accumulator = PipelinePropertyAccumulator::start_from_source(&consumer_source);
        let build = accumulator.close_with_sink(&consumer_sink);
        let consumer = PipelineId::new(pipelines.len());
        pipelines.push(PipelineSpec {
            id: consumer,
            source: consumer_source,
            transforms: Vec::new(),
            sink: consumer_sink,
            sink_sharing: SinkSharing::Exclusive,
            properties: build,
            output: row_type.clone(),
        });
        self.handles.add_consumer(handle, consumer)?;
        let root = self.pipeline_root_for(consumer)?;

        let graph = PipelineGraph {
            pipelines,
            dependencies: vec![PipelineDependency {
                producer,
                consumer,
                kind: DependencyKind::MaterializeBeforeRead,
            }],
            handles: mem::take(&mut self.handles).finish(),
            control_regions: mem::take(&mut self.control_regions),
            root,
        };
        graph.validate()?;
        Ok(graph)
    }
}
