// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
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
                required: Default::default(),
            }),
            sharing,
            self.plan.node(root).output.clone(),
            pipelines,
            dependencies,
        )
    }
}
