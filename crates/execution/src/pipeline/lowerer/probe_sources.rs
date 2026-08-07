// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Probe-source materialization and scan-filter attachment.

use super::*;

impl PipelineLowerer<'_> {
    pub(crate) fn attach_hash_join_runtime_filters(
        &self,
        mut source: SourceSpec,
        transforms: &[TransformSpec],
        handle: BreakerHandleId,
        spec: &HashJoinSpec,
    ) -> SourceSpec {
        if !can_push_hash_join_runtime_filter(spec.join_type) {
            return source;
        }
        let SourceSpec::Rowset(rowset) = &mut source else {
            return source;
        };
        for (build_key_index, condition) in spec.conditions.iter().enumerate() {
            if condition.comparison != JoinComparisonType::Equal {
                continue;
            }
            let Expression::Reference(reference) = &condition.left else {
                continue;
            };
            let Some(source_index) = trace_probe_reference_to_source(reference.index, transforms)
            else {
                continue;
            };
            let Some(&probe_column_id) = rowset.scan.column_ids.get(source_index) else {
                continue;
            };
            let Ok(probe_column_id) = u32::try_from(probe_column_id) else {
                continue;
            };
            rowset.add_dynamic_runtime_filter(RowsetDynamicRuntimeFilterSpec {
                handle,
                build_key_index,
                probe_column_id,
            });
        }
        source
    }

    pub(crate) fn collect_probe_roles_source_fallback(
        &mut self,
        root: PhysicalPlanNodeId,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(SourceSpec, Vec<TransformSpec>, Vec<PendingProbeBuild>)> {
        let output = self.plan.node(root).output.clone();
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            output.clone(),
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            root,
            SinkSpec::Materialize(MaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            output,
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        let source = SourceSpec::Materialized(MaterializedSourceSpec { handle });
        Ok((
            source,
            Vec::new(),
            vec![PendingProbeBuild { producer, handle }],
        ))
    }
}

fn can_push_hash_join_runtime_filter(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Inner | JoinType::Semi)
}

/// Trace a downstream join-key reference back to the rowset source.
///
/// A chained inner/semi hash probe emits its projected left columns before
/// any build payload, so a reference inside `left_projection` has exact
/// lineage to the preceding transform. Other transforms are deliberate
/// barriers: crossing one would require its own expression-lineage proof and
/// could move a dynamic predicate across a limit or volatile expression.
fn trace_probe_reference_to_source(
    mut reference_index: usize,
    transforms: &[TransformSpec],
) -> Option<usize> {
    for transform in transforms.iter().rev() {
        let TransformSpec::HashJoinProbe(probe) = transform else {
            return None;
        };
        if !matches!(probe.join_type, JoinType::Inner | JoinType::Semi) {
            return None;
        }
        reference_index = *probe.left_projection.get(reference_index)?;
    }
    Some(reference_index)
}
