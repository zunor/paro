// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    /// Lower the Halloween-protection barrier as two pipelines. The write sink
    /// can only start after the complete mutation input has been materialized.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_mutation_input_spool_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let child = self.only_child(root)?;
        let row_type = self.plan.node(child).output.clone();
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            row_type.clone(),
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            child,
            SinkSpec::Materialize(MaterializeSinkSpec { handle }),
            SinkSharing::Exclusive,
            row_type,
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;

        let source = SourceSpec::Materialized(MaterializedSourceSpec { handle });
        let consumer = self
            .push_pipeline(source, transforms, sink, sink_sharing, output, pipelines)?
            .tail;
        self.handles.add_consumer(handle, consumer)?;
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::MaterializeBeforeRead,
        });
        Ok(consumer)
    }
}
