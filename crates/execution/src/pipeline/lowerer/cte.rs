// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_materialized_cte_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: MaterializedCteSpec,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [producer_root, consumer_root] = children else {
            return Err(paro_error::internal(format!(
                "{} expected CTE producer and consumer children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let cte_row_type = RowType::new(spec.column_names.to_vec(), spec.column_types.to_vec());
        let handle =
            self.handles
                .register(BreakerHandleKind::Cte, cte_row_type, Default::default());
        let producer = self.lower_subtree_to_sink(
            *producer_root,
            SinkSpec::CteMaterialize(CteMaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            self.plan.node(*producer_root).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;

        let previous_handle = self.cte_handles.insert(spec.cte_index, handle);
        let previous_producer = self.cte_producers.insert(handle, producer);
        let consumer = self.lower_subtree_to_sink(
            *consumer_root,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        );

        if let Some(previous) = previous_handle {
            self.cte_handles.insert(spec.cte_index, previous);
        } else {
            self.cte_handles.remove(&spec.cte_index);
        }
        if let Some(previous) = previous_producer {
            self.cte_producers.insert(handle, previous);
        } else {
            self.cte_producers.remove(&handle);
        }

        consumer
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_recursive_cte_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: RecursiveCteSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [anchor_root, recursive_root] = children else {
            return Err(paro_error::internal(format!(
                "{} expected anchor and recursive children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let row_type = RowType::new(spec.column_names.to_vec(), spec.column_types.to_vec());
        let working = self.handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type.clone(),
            Default::default(),
        );
        let intermediate = self.handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type.clone(),
            Default::default(),
        );
        let accumulated = self.handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type.clone(),
            Default::default(),
        );
        let dedup = !spec.union_all;

        let append_sink = |handle| {
            SinkSpec::RecursiveTableAppend(RecursiveTableAppendSinkSpec {
                handle,
                required: Default::default(),
            })
        };
        let anchor = self.lower_subtree_to_sink(
            *anchor_root,
            append_sink(intermediate),
            SinkSharing::Exclusive,
            self.plan.node(*anchor_root).output.clone(),
            pipelines,
            dependencies,
        )?;

        let previous_recursive = self.recursive_cte_handles.insert(spec.cte_index, working);
        let first_recursive_pipeline = pipelines.len();
        let recursive_result = self.lower_subtree_to_sink(
            *recursive_root,
            append_sink(intermediate),
            SinkSharing::Exclusive,
            self.plan.node(*recursive_root).output.clone(),
            pipelines,
            dependencies,
        );
        if let Some(previous) = previous_recursive {
            self.recursive_cte_handles.insert(spec.cte_index, previous);
        } else {
            self.recursive_cte_handles.remove(&spec.cte_index);
        }
        let recursive_sink = recursive_result?;
        let recursive = (first_recursive_pipeline..pipelines.len())
            .map(PipelineId::new)
            .collect::<Vec<_>>();

        let source = SourceSpec::RecursiveTableScan(RecursiveTableScanSourceSpec {
            handle: accumulated,
        });
        let emit_source_handles = source.clone();
        let pushed = self.push_pipeline(
            source,
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        self.add_source_handle_dependencies(&emit_source_handles, pushed.entry, dependencies)?;
        let emit = pushed.tail;

        let region_id = ControlRegionId::new(self.control_regions.len());
        dependencies.push(PipelineDependency {
            producer: anchor,
            consumer: recursive_sink,
            kind: DependencyKind::LoopEntry(region_id),
        });
        dependencies.push(PipelineDependency {
            producer: recursive_sink,
            consumer: recursive_sink,
            kind: DependencyKind::LoopBack(region_id),
        });

        self.control_regions
            .push(ControlRegion::RecursiveCte(RecursiveCteRegion {
                anchor,
                recursive,
                emit,
                working,
                intermediate,
                accumulated: Some(accumulated),
                termination: RecursiveTermination::UntilEmpty,
                dedup: if dedup {
                    RecursiveCteDedup::HashSet
                } else {
                    RecursiveCteDedup::None
                },
            }));
        self.control_region_roots.insert(emit, region_id);
        Ok(emit)
    }
}
