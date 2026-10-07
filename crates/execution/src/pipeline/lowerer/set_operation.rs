// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_union_all_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let children = self.plan.child_ids(&self.plan.node(root).children);
        let [left, right] = children else {
            return Err(paro_error::internal(format!(
                "{} expected exactly two UNION ALL children, got {}",
                self.plan.node(root).label.display_name,
                children.len()
            )));
        };
        let shared = match sink_sharing {
            SinkSharing::Exclusive => SinkSharing::Shared(self.next_shared_sink()),
            shared @ SinkSharing::Shared(_) => shared,
        };
        let left_producer = self.lower_subtree_with_consumer_transforms_to_sink(
            *left,
            transforms.clone(),
            sink.clone(),
            shared,
            output.clone(),
            pipelines,
            dependencies,
        )?;
        let right_producer = self.lower_subtree_with_consumer_transforms_to_sink(
            *right,
            transforms,
            sink.clone(),
            shared,
            output.clone(),
            pipelines,
            dependencies,
        )?;
        // A zero-row producer is the explicit multi-input completion token.
        // Both data branches remain independently schedulable; the fence runs
        // only after they finish and, as the final shared-sink participant,
        // seals the downstream state before its single producer id is exposed
        // to consumers.
        let fence = self.push_pipeline(
            SourceSpec::Empty(EmptyResultSpec),
            Vec::new(),
            sink,
            shared,
            output,
            pipelines,
        )?;
        dependencies.push(PipelineDependency {
            producer: left_producer,
            consumer: fence.entry,
            kind: DependencyKind::SharedSinkInput,
        });
        dependencies.push(PipelineDependency {
            producer: right_producer,
            consumer: fence.entry,
            kind: DependencyKind::SharedSinkInput,
        });
        Ok(fence.tail)
    }

    pub(crate) fn lower_set_operation_input(
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
            }),
            sharing,
            self.plan.node(root).output.clone(),
            pipelines,
            dependencies,
        )
    }
}
