// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_pipeline(
        &mut self,
        mut source: SourceSpec,
        mut transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        mut output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineChain> {
        const MAX_REPAIR_LOWERING_STEPS: usize = 16;

        let mut entry = None;
        let mut pending_repair_source = None;

        for _ in 0..MAX_REPAIR_LOWERING_STEPS {
            let build = self.build_pipeline_properties(&source, &transforms, &sink);
            let Some(repair) = build.repair.repairs.into_iter().next() else {
                let id = self.push_pipeline_unrepaired(
                    source,
                    transforms,
                    sink,
                    sink_sharing,
                    output,
                    build.properties,
                    pipelines,
                );
                if entry.is_none() {
                    entry = Some(id);
                }
                self.attach_pending_repair_source(pending_repair_source.take(), id, dependencies)?;
                return Ok(PipelineChain {
                    entry: entry.expect("pipeline chain must have an entry stage"),
                    tail: id,
                });
            };

            match repair {
                PropertyRepairKind::BatchIndexAdapter | PropertyRepairKind::SingleTaskFallback => {
                    transforms.push(repair_transform(repair));
                }
                PropertyRepairKind::Sort(ordering) => {
                    let handle = self.handles.register(
                        BreakerHandleKind::Sort,
                        output.clone(),
                        Default::default(),
                    );
                    let producer_sink = SinkSpec::SortBuild(SortBuildSinkSpec {
                        handle,
                        orders: sort_orders_from_ordering_spec(&ordering, &output)?,
                        projection_map: identity_projection(&output),
                        input_types: output.types.clone(),
                        output_names: output.names.clone(),
                        output_types: output.types.clone(),
                        force_external: false,
                        required: Default::default(),
                    });
                    let producer_build =
                        self.build_pipeline_properties(&source, &transforms, &producer_sink);
                    let producer = self.push_pipeline_unrepaired(
                        source,
                        transforms,
                        producer_sink,
                        SinkSharing::Exclusive,
                        output.clone(),
                        producer_build.properties,
                        pipelines,
                    );
                    if entry.is_none() {
                        entry = Some(producer);
                    }
                    self.attach_pending_repair_source(
                        pending_repair_source.take(),
                        producer,
                        dependencies,
                    )?;
                    self.handles.set_producer(handle, producer)?;

                    source = SourceSpec::SortEmit(SortEmitSourceSpec {
                        handle,
                        ordering,
                        output_names: output.names.clone(),
                        output_types: output.types.clone(),
                    });
                    transforms = Vec::new();
                    pending_repair_source = Some(PendingRepairSource {
                        handle,
                        producer,
                        kind: DependencyKind::FinalizeBeforeEmit,
                    });
                }
            }
            output = source.output_row_type(&output);
        }

        Err(paro_error::internal(
            "property repair lowering did not converge after inserting breaker pipelines",
        ))
    }

    fn build_pipeline_properties(
        &self,
        source: &SourceSpec,
        transforms: &[TransformSpec],
        sink: &SinkSpec,
    ) -> crate::pipeline::properties::PipelinePropertyBuild {
        let mut accumulator = PipelinePropertyAccumulator::start_from_source(&source);
        for transform in transforms {
            accumulator.apply_transform(transform);
        }
        accumulator.close_with_sink(sink)
    }

    #[allow(clippy::too_many_arguments)]
    fn push_pipeline_unrepaired(
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

    fn attach_pending_repair_source(
        &mut self,
        pending: Option<PendingRepairSource>,
        consumer: PipelineId,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<()> {
        let Some(pending) = pending else {
            return Ok(());
        };
        self.handles.add_consumer(pending.handle, consumer)?;
        dependencies.push(PipelineDependency {
            producer: pending.producer,
            consumer,
            kind: pending.kind,
        });
        Ok(())
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

struct PendingRepairSource {
    handle: BreakerHandleId,
    producer: PipelineId,
    kind: DependencyKind,
}

fn identity_projection(row_type: &RowType) -> Box<[usize]> {
    (0..row_type.column_count())
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn sort_orders_from_ordering_spec(
    ordering: &OrderingSpec,
    row_type: &RowType,
) -> Result<Box<[OrderByNode]>> {
    if ordering.columns.is_empty() {
        return Err(paro_error::internal(
            "property repair sort requires at least one ordering column",
        ));
    }

    let mut orders = Vec::with_capacity(ordering.columns.len());
    for column in &ordering.columns {
        let data_type = row_type.types.get(column.column).cloned().ok_or_else(|| {
            paro_error::internal(format!(
                "property repair sort references missing column {}",
                column.column
            ))
        })?;
        orders.push(OrderByNode {
            expression: Expression::Reference(ReferenceExpression::new(column.column, data_type)),
            ascending: matches!(column.direction, OrderingDirection::Asc),
            nulls_first: matches!(column.nulls, NullOrdering::First),
        });
    }
    Ok(orders.into_boxed_slice())
}
