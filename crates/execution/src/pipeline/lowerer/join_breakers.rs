// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_join_subtree_with_consumer_transforms(
        &mut self,
        join_root: PhysicalPlanNodeId,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        if consumer_transforms.is_empty() {
            return self.lower_subtree_to_sink(
                join_root,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            );
        }
        let node = self.plan.node(join_root);
        let breaker = match &node.kind {
            PhysicalNodeKind::HashJoin(spec) => BreakerDispatch::HashJoin(spec.clone()),
            PhysicalNodeKind::NestedLoopJoin(spec) => BreakerDispatch::NestedLoopJoin(spec.clone()),
            PhysicalNodeKind::IEJoin(spec) => BreakerDispatch::IEJoin(spec.clone()),
            PhysicalNodeKind::CrossProduct(spec) => BreakerDispatch::CrossProduct(spec.clone()),
            _ => {
                let (source, mut transforms) = self.collect_linear_roles(join_root)?;
                let source_handles = source.clone();
                transforms.extend(consumer_transforms);
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
                return Ok(pushed.tail);
            }
        };
        self.dispatch_breaker_to_sink(
            join_root,
            breaker,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_delim_join_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: DelimJoinSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [capture_root, join_root] = children else {
            return Err(paro_error::internal(format!(
                "{} expected delim capture and wrapped join children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let delim_row_type = RowType::new(
            (0..spec.duplicate_keys.len())
                .map(|idx| format!("delim_{}", idx + 1))
                .collect(),
            spec.duplicate_keys
                .iter()
                .map(Expression::return_type)
                .collect(),
        );
        let capture_row_type = self.plan.node(*capture_root).output.clone();
        let delim_values =
            self.handles
                .register(BreakerHandleKind::Delim, delim_row_type, Default::default());
        let cached_outer = self.handles.register(
            BreakerHandleKind::Delim,
            capture_row_type.clone(),
            Default::default(),
        );

        let capture = self.lower_subtree_to_sink(
            *capture_root,
            SinkSpec::DelimCapture(DelimCaptureSinkSpec {
                handle: delim_values,
                duplicate_keys: spec.duplicate_keys.clone(),
                cached_outer: Some(cached_outer),
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            capture_row_type,
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(delim_values, capture)?;
        self.handles.set_producer(cached_outer, capture)?;

        let delim_table_indexes = self.collect_delim_scan_table_indexes(*join_root)?;
        let mut previous_values = Vec::with_capacity(delim_table_indexes.len());
        for table_index in delim_table_indexes {
            previous_values.push((
                table_index,
                self.delim_value_handles.insert(table_index, delim_values),
            ));
        }
        self.cached_outer_handles.push(cached_outer);

        let first_join_pipeline = pipelines.len();
        let join = self.lower_join_subtree_with_consumer_transforms(
            *join_root,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        );

        self.cached_outer_handles.pop();
        for (table_index, previous) in previous_values {
            if let Some(previous) = previous {
                self.delim_value_handles.insert(table_index, previous);
            } else {
                self.delim_value_handles.remove(&table_index);
            }
        }

        let join = join?;
        let dependent_roots =
            self.dependent_subgraph_roots(first_join_pipeline, pipelines.len(), join)?;
        let region_id = ControlRegionId::new(self.control_regions.len());
        self.control_regions.push(ControlRegion::CorrelatedSubquery(
            CorrelatedSubqueryRegion {
                side: match spec.side {
                    DelimJoinSideSpec::Left => DelimJoinSide::Left,
                    DelimJoinSideSpec::Right => DelimJoinSide::Right,
                },
                capture,
                dependent_roots,
                join,
                delim_values,
                cached_outer: Some(cached_outer),
            },
        ));
        self.control_region_roots.insert(join, region_id);
        Ok(join)
    }
}
