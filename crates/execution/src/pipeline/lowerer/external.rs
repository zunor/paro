// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_external_table_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &ExternalTableSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let handle = self.handles.register(
            BreakerHandleKind::ExternalTable,
            output.clone(),
            Default::default(),
        );
        let producer_sink = SinkSpec::ExternalTable(ExternalTableSinkSpec {
            handle,
            spec: spec.clone(),
        });
        let producer = match self.plan.child_ids(&self.plan.node(root).children) {
            [child] => self.lower_subtree_to_sink(
                *child,
                producer_sink,
                SinkSharing::Exclusive,
                self.plan.node(*child).output.clone(),
                pipelines,
                dependencies,
            )?,
            [] => {
                let source = SourceSpec::Dummy(crate::physical::specs::DummyScanSpec);
                let producer_output = RowType::new(Vec::new(), Vec::new());
                let pushed = self.push_pipeline(
                    source,
                    Vec::new(),
                    producer_sink,
                    SinkSharing::Exclusive,
                    producer_output,
                    pipelines,
                )?;
                pushed.tail
            }
            children => {
                return Err(paro_error::internal(format!(
                    "EXTERNAL_TABLE expected zero or one child, got {}",
                    children.len()
                )));
            }
        };
        self.handles.set_producer(handle, producer)?;

        let consumer_source = SourceSpec::ExternalTable(ExternalTableSourceSpec {
            handle,
            output_names: output.names.clone(),
            output_types: output.types.clone(),
        });
        let pushed = self.push_pipeline(
            consumer_source,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
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
}
