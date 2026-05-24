// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_set_operation_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &SetOperationSpec,
        mut consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [left, right] = children else {
            return Err(paro_error::internal(format!(
                "{} expected exactly two set-operation children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let handle = self.handles.register(
            BreakerHandleKind::SetOperation,
            self.plan.node(root).output.clone(),
            Default::default(),
        );
        let shared = SinkSharing::Shared(self.next_shared_sink());

        let left_producer = self.lower_set_operation_input(
            *left,
            handle,
            spec,
            SetOperationInputSide::Left,
            shared,
            pipelines,
            dependencies,
        )?;
        let right_producer = self.lower_set_operation_input(
            *right,
            handle,
            spec,
            SetOperationInputSide::Right,
            shared,
            pipelines,
            dependencies,
        )?;

        let consumer_source = SourceSpec::SetOperationEmit(SetOperationEmitSourceSpec {
            handle,
            spec: spec.clone(),
        });
        let mut accumulator = PipelinePropertyAccumulator::start_from_source(&consumer_source);
        for transform in &consumer_transforms {
            accumulator.apply_transform(transform);
        }
        let build = accumulator.close_with_sink(&sink);
        for repair in build.repair.repairs {
            consumer_transforms.push(repair_transform(repair));
        }
        let consumer = PipelineId::new(pipelines.len());
        pipelines.push(PipelineSpec {
            id: consumer,
            source: consumer_source,
            transforms: consumer_transforms,
            sink,
            sink_sharing,
            properties: build.properties,
            output,
        });
        self.handles.add_consumer(handle, consumer)?;
        dependencies.push(PipelineDependency {
            producer: left_producer,
            consumer,
            kind: DependencyKind::FinalizeBeforeEmit,
        });
        dependencies.push(PipelineDependency {
            producer: right_producer,
            consumer,
            kind: DependencyKind::FinalizeBeforeEmit,
        });
        Ok(consumer)
    }

    fn lower_set_operation_input(
        &mut self,
        root: PhysicalPlanNodeId,
        handle: BreakerHandleId,
        spec: &SetOperationSpec,
        side: SetOperationInputSide,
        sharing: SinkSharing,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        self.lower_subtree_to_sink(
            root,
            SinkSpec::SetOperationInput(SetOperationInputSinkSpec {
                handle,
                spec: spec.clone(),
                side,
                required: Default::default(),
            }),
            sharing,
            self.plan.node(root).output.clone(),
            pipelines,
            dependencies,
        )
    }
}
