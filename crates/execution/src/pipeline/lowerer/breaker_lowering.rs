// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Breaker-specific lowering dispatch.

use super::*;

pub(crate) enum BreakerDispatch {
    TopN(TopNSpec),
    Sort(SortSpec),
    Aggregate(AggregateSpec),
    SetOperation(SetOperationSpec),
    Window(WindowSpec),
    PartitionAggregateWindow(PartitionAggregateWindowSpec),
    HashJoin(HashJoinSpec),
    NestedLoopJoin(NestedLoopJoinSpec),
    SortRangeJoin(SortRangeJoinSpec),
    ClassicIeJoin(ClassicIeJoinSpec),
    CrossProduct(CrossProductSpec),
    ExternalTable(ExternalTableSpec),
}

enum EmitBreakerBuild {
    TopN(TopNSpec),
    Sort(SortSpec),
    Aggregate(AggregateSpec),
    SetOperation(SetOperationSpec),
    Window(WindowSpec),
    PartitionAggregateWindow(PartitionAggregateWindowSpec),
}

impl EmitBreakerBuild {
    /// Split an owned breaker into its build description and prospective emit
    /// source. The source initially carries a harmless placeholder handle so
    /// its authoritative properties can be checked before lowering mutates
    /// the handle catalog or pipeline graph.
    fn from_dispatch(breaker: BreakerDispatch, output: &RowType) -> Option<(Self, SourceSpec)> {
        let placeholder = BreakerHandleId::new(0);
        Some(match breaker {
            BreakerDispatch::Aggregate(spec) => {
                let source = aggregate_emit_source_spec(placeholder, spec.clone());
                (Self::Aggregate(spec), source)
            }
            BreakerDispatch::TopN(spec) => {
                let source = SourceSpec::TopNEmit(TopNEmitSourceSpec {
                    handle: placeholder,
                    spec: spec.clone(),
                });
                (Self::TopN(spec), source)
            }
            BreakerDispatch::Sort(spec) => {
                let source = SourceSpec::SortEmit(SortEmitSourceSpec {
                    handle: placeholder,
                    ordering: ordering_spec_from_orders(&spec.orders),
                    output_names: output.names.clone(),
                    output_types: output.types.clone(),
                });
                (Self::Sort(spec), source)
            }
            BreakerDispatch::Window(spec) => {
                let source = SourceSpec::WindowEmit(WindowEmitSourceSpec {
                    handle: placeholder,
                    spec: spec.clone(),
                });
                (Self::Window(spec), source)
            }
            BreakerDispatch::PartitionAggregateWindow(spec) => {
                let source = SourceSpec::PartitionAggregateWindowEmit(
                    PartitionAggregateWindowEmitSourceSpec {
                        handle: placeholder,
                        spec: spec.clone(),
                    },
                );
                (Self::PartitionAggregateWindow(spec), source)
            }
            BreakerDispatch::SetOperation(spec) => {
                let source = SourceSpec::SetOperationEmit(SetOperationEmitSourceSpec {
                    handle: placeholder,
                    spec: spec.clone(),
                });
                (Self::SetOperation(spec), source)
            }
            BreakerDispatch::HashJoin(_)
            | BreakerDispatch::NestedLoopJoin(_)
            | BreakerDispatch::SortRangeJoin(_)
            | BreakerDispatch::ClassicIeJoin(_)
            | BreakerDispatch::CrossProduct(_)
            | BreakerDispatch::ExternalTable(_) => return None,
        })
    }

    fn handle_kind(&self) -> BreakerHandleKind {
        match self {
            Self::TopN(_) => BreakerHandleKind::TopN,
            Self::Sort(_) => BreakerHandleKind::Sort,
            Self::Aggregate(_) => BreakerHandleKind::Aggregate,
            Self::SetOperation(_) => BreakerHandleKind::SetOperation,
            Self::Window(_) => BreakerHandleKind::Window,
            Self::PartitionAggregateWindow(_) => BreakerHandleKind::PartitionAggregateWindow,
        }
    }
}

fn assign_emit_source_handle(source: &mut SourceSpec, handle: BreakerHandleId) -> Result<()> {
    match source {
        SourceSpec::HashAggregateEmit(spec) => spec.handle = handle,
        SourceSpec::UngroupedAggregateEmit(spec) => spec.handle = handle,
        SourceSpec::PerfectHashAggregateEmit(spec) => spec.handle = handle,
        SourceSpec::TopNEmit(spec) => spec.handle = handle,
        SourceSpec::SortEmit(spec) => spec.handle = handle,
        SourceSpec::WindowEmit(spec) => spec.handle = handle,
        SourceSpec::PartitionAggregateWindowEmit(spec) => spec.handle = handle,
        SourceSpec::SetOperationEmit(spec) => spec.handle = handle,
        _ => {
            return Err(paro_error::internal(
                "emit breaker produced a source without a breaker handle",
            ))
        }
    }
    Ok(())
}

impl<'a> PipelineLowerer<'a> {
    fn lower_emit_breaker_source(
        &mut self,
        root: PhysicalPlanNodeId,
        breaker: BreakerDispatch,
        require_parallel_probe: bool,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<Option<BreakerProbeSource>> {
        let output = self.plan.node(root).output.clone();
        let Some((build, mut source)) = EmitBreakerBuild::from_dispatch(breaker, &output) else {
            return Ok(None);
        };
        if require_parallel_probe && !source_supports_parallel_probe_fusion(&source) {
            return Ok(None);
        }

        let handle = self
            .handles
            .register(build.handle_kind(), output.clone(), Default::default());
        assign_emit_source_handle(&mut source, handle)?;
        let pending = match build {
            EmitBreakerBuild::Aggregate(spec) => {
                let child = self.only_child(root)?;
                let producer = self.lower_subtree_to_sink(
                    child,
                    aggregate_build_sink_spec(handle, spec),
                    SinkSharing::Exclusive,
                    self.plan.node(child).output.clone(),
                    pipelines,
                    dependencies,
                )?;
                self.handles.set_producer(handle, producer)?;
                vec![PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::FinalizeBeforeEmit,
                }]
            }
            EmitBreakerBuild::TopN(spec) => {
                ensure_streaming_topn_supported(&spec)?;
                let child = self.only_child(root)?;
                let producer = self.lower_subtree_to_sink(
                    child,
                    SinkSpec::TopNBuild(TopNBuildSinkSpec {
                        handle,
                        spec,
                        required: Default::default(),
                    }),
                    SinkSharing::Exclusive,
                    self.plan.node(child).output.clone(),
                    pipelines,
                    dependencies,
                )?;
                self.handles.set_producer(handle, producer)?;
                vec![PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::FinalizeBeforeEmit,
                }]
            }
            EmitBreakerBuild::Sort(spec) => {
                let child = self.only_child(root)?;
                let input = self.plan.node(child).output.clone();
                let producer = self.lower_subtree_to_sink(
                    child,
                    SinkSpec::SortBuild(SortBuildSinkSpec {
                        handle,
                        orders: spec.orders.clone(),
                        projection_map: spec.projection_map.clone(),
                        input_types: input.types.clone(),
                        output_names: output.names.clone(),
                        output_types: output.types.clone(),
                        force_external: false,
                        required: Default::default(),
                    }),
                    SinkSharing::Exclusive,
                    input,
                    pipelines,
                    dependencies,
                )?;
                self.handles.set_producer(handle, producer)?;
                vec![PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::FinalizeBeforeEmit,
                }]
            }
            EmitBreakerBuild::Window(spec) => {
                let child = self.only_child(root)?;
                let producer = self.lower_subtree_to_sink(
                    child,
                    SinkSpec::WindowBuild(WindowBuildSinkSpec {
                        handle,
                        spec,
                        required: Default::default(),
                    }),
                    SinkSharing::Exclusive,
                    self.plan.node(child).output.clone(),
                    pipelines,
                    dependencies,
                )?;
                self.handles.set_producer(handle, producer)?;
                vec![PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::FinalizeBeforeEmit,
                }]
            }
            EmitBreakerBuild::PartitionAggregateWindow(spec) => {
                spec.verify()?;
                let child = self.only_child(root)?;
                let producer = self.lower_subtree_to_sink(
                    child,
                    SinkSpec::PartitionAggregateWindowBuild(
                        PartitionAggregateWindowBuildSinkSpec {
                            handle,
                            spec,
                            required: Default::default(),
                        },
                    ),
                    SinkSharing::Exclusive,
                    self.plan.node(child).output.clone(),
                    pipelines,
                    dependencies,
                )?;
                self.handles.set_producer(handle, producer)?;
                vec![PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::FinalizeBeforeEmit,
                }]
            }
            EmitBreakerBuild::SetOperation(spec) => {
                let children = self.plan.child_ids(&self.plan.node(root).children);
                let [left, right] = children else {
                    return Err(paro_error::internal(format!(
                        "{} expected exactly two set-operation children, got {}",
                        self.plan.node(root).label.display_name,
                        children.len()
                    )));
                };
                let shared = SinkSharing::Shared(self.next_shared_sink());
                let left_producer = self.lower_set_operation_input(
                    *left,
                    handle,
                    &spec,
                    SetOperationInputSide::Left,
                    shared,
                    pipelines,
                    dependencies,
                )?;
                let right_producer = self.lower_set_operation_input(
                    *right,
                    handle,
                    &spec,
                    SetOperationInputSide::Right,
                    shared,
                    pipelines,
                    dependencies,
                )?;
                [left_producer, right_producer]
                    .into_iter()
                    .map(|producer| PendingProbeDependency {
                        producer,
                        handle,
                        kind: DependencyKind::FinalizeBeforeEmit,
                    })
                    .collect()
            }
        };
        Ok(Some(BreakerProbeSource {
            source,
            dependencies: pending,
        }))
    }

    /// Expose only emit sources that retain Materialized's unbounded reader
    /// parallelism. Single-task emitters intentionally keep the materialized
    /// boundary because its parallel readers are the scheduling adapter for
    /// the downstream probe chain.
    pub(crate) fn lower_breaker_to_probe_source(
        &mut self,
        root: PhysicalPlanNodeId,
        breaker: BreakerDispatch,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<Option<BreakerProbeSource>> {
        self.lower_emit_breaker_source(root, breaker, true, pipelines, dependencies)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_emit_breaker_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        breaker: BreakerDispatch,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let probe_source = self
            .lower_emit_breaker_source(root, breaker, false, pipelines, dependencies)?
            .ok_or_else(|| paro_error::internal("requested emit source for a non-emit breaker"))?;
        let pushed = self.push_pipeline(
            probe_source.source,
            transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        for pending in probe_source.dependencies {
            self.handles.add_consumer(pending.handle, pushed.entry)?;
            dependencies.push(PipelineDependency {
                producer: pending.producer,
                consumer: pushed.entry,
                kind: pending.kind,
            });
        }
        Ok(pushed.tail)
    }

    pub(crate) fn breaker_dispatch_for_root(
        &self,
        root: PhysicalPlanNodeId,
    ) -> Option<BreakerDispatch> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::TopN(spec) => Some(BreakerDispatch::TopN(spec.clone())),
            PhysicalNodeKind::Sort(spec) => Some(BreakerDispatch::Sort(spec.clone())),
            PhysicalNodeKind::Aggregate(spec) => Some(BreakerDispatch::Aggregate(spec.clone())),
            PhysicalNodeKind::SetOperation(spec) => {
                Some(BreakerDispatch::SetOperation(spec.clone()))
            }
            PhysicalNodeKind::Window(spec) if !is_streaming_window_supported(spec) => {
                Some(BreakerDispatch::Window(spec.clone()))
            }
            PhysicalNodeKind::PartitionAggregateWindow(spec) => {
                Some(BreakerDispatch::PartitionAggregateWindow(spec.clone()))
            }
            PhysicalNodeKind::HashJoin(spec) => Some(BreakerDispatch::HashJoin(spec.clone())),
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                Some(BreakerDispatch::NestedLoopJoin(spec.clone()))
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                Some(BreakerDispatch::SortRangeJoin(spec.clone()))
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                Some(BreakerDispatch::ClassicIeJoin(spec.clone()))
            }
            PhysicalNodeKind::CrossProduct(spec) => {
                Some(BreakerDispatch::CrossProduct(spec.clone()))
            }
            PhysicalNodeKind::ExternalTable(spec) => {
                Some(BreakerDispatch::ExternalTable(spec.clone()))
            }
            _ => None,
        }
    }

    pub(crate) fn tail_breaker_dispatch(
        &self,
        root: PhysicalPlanNodeId,
    ) -> Result<BreakerDispatch> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::TopN(spec) => Ok(BreakerDispatch::TopN(spec.clone())),
            PhysicalNodeKind::Sort(spec) => Ok(BreakerDispatch::Sort(spec.clone())),
            PhysicalNodeKind::Aggregate(spec) => Ok(BreakerDispatch::Aggregate(spec.clone())),
            PhysicalNodeKind::SetOperation(spec) => Ok(BreakerDispatch::SetOperation(spec.clone())),
            PhysicalNodeKind::Window(spec) => Ok(BreakerDispatch::Window(spec.clone())),
            PhysicalNodeKind::PartitionAggregateWindow(spec) => {
                Ok(BreakerDispatch::PartitionAggregateWindow(spec.clone()))
            }
            PhysicalNodeKind::HashJoin(spec) => Ok(BreakerDispatch::HashJoin(spec.clone())),
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                Ok(BreakerDispatch::NestedLoopJoin(spec.clone()))
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                Ok(BreakerDispatch::SortRangeJoin(spec.clone()))
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                Ok(BreakerDispatch::ClassicIeJoin(spec.clone()))
            }
            PhysicalNodeKind::CrossProduct(spec) => Ok(BreakerDispatch::CrossProduct(spec.clone())),
            PhysicalNodeKind::ExternalTable(spec) => {
                Ok(BreakerDispatch::ExternalTable(spec.clone()))
            }
            _ => Err(paro_error::internal(
                "collect_tail_to_breaker returned a non-breaker node",
            )),
        }
    }

    pub(crate) fn is_tail_breaker(kind: &PhysicalNodeKind) -> bool {
        match kind {
            PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::Sort(_)
            | PhysicalNodeKind::HashJoin(_)
            | PhysicalNodeKind::NestedLoopJoin(_)
            | PhysicalNodeKind::SortRangeJoin(_)
            | PhysicalNodeKind::ClassicIeJoin(_)
            | PhysicalNodeKind::CrossProduct(_)
            | PhysicalNodeKind::ExternalTable(_)
            | PhysicalNodeKind::SetOperation(_)
            | PhysicalNodeKind::DelimJoin(_)
            | PhysicalNodeKind::RecursiveCte(_) => true,
            PhysicalNodeKind::Aggregate(_) => true,
            PhysicalNodeKind::Window(spec) => !is_streaming_window_supported(spec),
            PhysicalNodeKind::PartitionAggregateWindow(_) => true,
            _ => false,
        }
    }

    /// Breakers with a native emit source. This is a cheap discriminator used
    /// before cloning an owned dispatch spec for probe-source consideration;
    /// scheduling eligibility is still decided from the constructed source's
    /// canonical pipeline properties.
    pub(crate) fn is_emit_breaker(kind: &PhysicalNodeKind) -> bool {
        match kind {
            PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::Sort(_)
            | PhysicalNodeKind::Aggregate(_)
            | PhysicalNodeKind::SetOperation(_)
            | PhysicalNodeKind::PartitionAggregateWindow(_) => true,
            PhysicalNodeKind::Window(spec) => !is_streaming_window_supported(spec),
            _ => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dispatch_breaker_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        breaker: BreakerDispatch,
        transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        match breaker {
            breaker @ (BreakerDispatch::TopN(_)
            | BreakerDispatch::Sort(_)
            | BreakerDispatch::Aggregate(_)
            | BreakerDispatch::SetOperation(_)
            | BreakerDispatch::Window(_)
            | BreakerDispatch::PartitionAggregateWindow(_)) => self.lower_emit_breaker_to_sink(
                root,
                breaker,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::HashJoin(spec) => self.lower_hash_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::NestedLoopJoin(spec) => self.lower_nested_loop_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::SortRangeJoin(spec) => self.lower_sort_range_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::ClassicIeJoin(spec) => self.lower_classic_ie_join_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::CrossProduct(spec) => self.lower_cross_product_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
            BreakerDispatch::ExternalTable(spec) => self.lower_external_table_to_sink(
                root,
                &spec,
                transforms,
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            ),
        }
    }
}
