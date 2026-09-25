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
        self.push_pipeline_with_lineage(
            source,
            transforms,
            sink,
            sink_sharing,
            output,
            PipelineOperatorLineage::default(),
            pipelines,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_pipeline_with_lineage(
        &mut self,
        source: SourceSpec,
        mut transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        mut operator_lineage: PipelineOperatorLineage,
        pipelines: &mut Vec<PipelineSpec>,
    ) -> Result<PipelineChain> {
        confirm_covering_runtime_filters(&source, &mut transforms);
        if operator_lineage.transforms.len() != transforms.len() {
            operator_lineage.transforms = std::iter::repeat_n(None, transforms.len()).collect();
        }
        let (transforms, transform_lineage) =
            fuse_adjacent_projects_with_lineage(transforms, operator_lineage.transforms);
        operator_lineage.transforms = transform_lineage.into_boxed_slice();
        let mut properties = self.build_pipeline_properties(&source, &transforms, &sink);
        properties.operator_lineage = operator_lineage;
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
        } else if let SourceSpec::Rowset(source) = source {
            for filter in &source.dynamic_runtime_filters {
                self.handles.add_consumer(filter.handle, consumer)?;
                let producer = self.handles.producer(filter.handle)?.ok_or_else(|| {
                    paro_error::internal("runtime-filter handle has no producer pipeline")
                })?;
                let dependency = PipelineDependency {
                    producer,
                    consumer,
                    kind: DependencyKind::BuildBeforeProbe,
                };
                if !dependencies.contains(&dependency) {
                    dependencies.push(dependency);
                }
            }
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

/// Grant probe-replacement authority from concrete filter installations.
/// Spill replay, CTE scans, and branches where lineage could not reach storage
/// never acquire that authority and retain the ordinary probe.
pub(super) fn confirm_covering_runtime_filters(
    source: &SourceSpec,
    transforms: &mut [TransformSpec],
) {
    for transform in transforms {
        let TransformSpec::HashJoinProbe(probe) = transform else {
            continue;
        };
        probe.covering_runtime_filter_key = None;
        let SourceSpec::Rowset(rowset) = source else {
            continue;
        };
        let mut covering = rowset
            .dynamic_runtime_filters
            .iter()
            .filter(|filter| {
                filter.handle == probe.handle
                    && filter.application == RuntimeFilterApplication::ProbeReplacementEligible
            })
            .map(|filter| filter.runtime_filter_key_index);
        let Some(key_index) = covering.next() else {
            continue;
        };
        if covering.all(|candidate| candidate == key_index) {
            probe.covering_runtime_filter_key = Some(key_index);
        }
    }
}

/// Fuse projection chains that only become adjacent after physical operators
/// are distributed into pipeline producers, notably UNION ALL fan-in.
#[cfg(test)]
pub(super) fn fuse_adjacent_projects(transforms: Vec<TransformSpec>) -> Vec<TransformSpec> {
    let mut fused = Vec::with_capacity(transforms.len());
    for transform in transforms {
        let TransformSpec::Project(outer) = transform else {
            fused.push(transform);
            continue;
        };
        let Some(TransformSpec::Project(inner)) = fused.last_mut() else {
            fused.push(TransformSpec::Project(outer));
            continue;
        };
        let Some(composed) = outer.compose_over(inner) else {
            fused.push(TransformSpec::Project(outer));
            continue;
        };
        *inner = composed;
    }
    fused
}

fn fuse_adjacent_projects_with_lineage(
    transforms: Vec<TransformSpec>,
    operator_lineage: Box<[Option<paro_planner::logical::plan::PlanNodeId>]>,
) -> (
    Vec<TransformSpec>,
    Vec<Option<paro_planner::logical::plan::PlanNodeId>>,
) {
    debug_assert_eq!(transforms.len(), operator_lineage.len());
    let mut fused = Vec::with_capacity(transforms.len());
    let mut lineage = Vec::with_capacity(transforms.len());
    for (transform, logical_node) in transforms.into_iter().zip(operator_lineage.into_vec()) {
        let TransformSpec::Project(outer) = transform else {
            fused.push(transform);
            lineage.push(logical_node);
            continue;
        };
        let Some(TransformSpec::Project(inner)) = fused.last_mut() else {
            fused.push(TransformSpec::Project(outer));
            lineage.push(logical_node);
            continue;
        };
        let Some(composed) = outer.compose_over(inner) else {
            fused.push(TransformSpec::Project(outer));
            lineage.push(logical_node);
            continue;
        };
        *inner = composed;
        // The fused executor performs two logical projections as one physical
        // transform.  Neither individual logical node is a sound one-to-one
        // execution coordinate, so keep the profile explicitly uncovered.
        if let Some(last) = lineage.last_mut() {
            *last = None;
        }
    }
    (fused, lineage)
}
