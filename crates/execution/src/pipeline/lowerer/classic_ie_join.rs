// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    fn lower_classic_ie_join_side(
        &mut self,
        child: PhysicalPlanNodeId,
        output_types: &[LogicalType],
        name_prefix: &str,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(PipelineId, BreakerHandleId)> {
        let row_type = RowType::new(
            (0..output_types.len())
                .map(|idx| format!("{name_prefix}_{}", idx + 1))
                .collect(),
            output_types.to_vec(),
        );
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            row_type,
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            child,
            SinkSpec::Materialize(MaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            self.plan.node(child).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        Ok((producer, handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_classic_ie_join_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &ClassicIeJoinSpec,
        consumer_transforms: Vec<TransformSpec>,
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
                "{} expected exactly two classic IE join children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let (left_producer, left_handle) = self.lower_classic_ie_join_side(
            *left,
            &spec.left_output_types,
            "classic_ie_left",
            pipelines,
            dependencies,
        )?;
        let (right_producer, right_handle) = self.lower_classic_ie_join_side(
            *right,
            &spec.right_output_types,
            "classic_ie_right",
            pipelines,
            dependencies,
        )?;

        let source = SourceSpec::ClassicIeJoin(ClassicIeJoinSourceSpec {
            left_handle,
            right_handle,
            spec: spec.clone(),
        });
        let pushed = self.push_pipeline(
            source,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        self.handles.add_consumer(left_handle, pushed.entry)?;
        self.handles.add_consumer(right_handle, pushed.entry)?;
        dependencies.push(PipelineDependency {
            producer: left_producer,
            consumer: pushed.entry,
            kind: DependencyKind::BuildBeforeProbe,
        });
        dependencies.push(PipelineDependency {
            producer: right_producer,
            consumer: pushed.entry,
            kind: DependencyKind::BuildBeforeProbe,
        });
        Ok(pushed.tail)
    }
}
