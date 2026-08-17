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

/// Owned breaker dispatch that passed the cheap borrowed precheck. The actual
/// constructed source remains authoritative for fusion eligibility.
pub(crate) struct ProbeFusionCandidateDispatch(BreakerDispatch);

enum BreakerRef<'a> {
    TopN(&'a TopNSpec),
    Sort(&'a SortSpec),
    Aggregate(&'a AggregateSpec),
    SetOperation(&'a SetOperationSpec),
    Window(&'a WindowSpec),
    PartitionAggregateWindow(&'a PartitionAggregateWindowSpec),
    HashJoin(&'a HashJoinSpec),
    NestedLoopJoin(&'a NestedLoopJoinSpec),
    SortRangeJoin(&'a SortRangeJoinSpec),
    ClassicIeJoin(&'a ClassicIeJoinSpec),
    CrossProduct(&'a CrossProductSpec),
    ExternalTable(&'a ExternalTableSpec),
    MaterializedCteControl,
    DelimJoinControl,
    RecursiveCteControl,
}

impl<'a> BreakerRef<'a> {
    fn from_kind(kind: &'a PhysicalNodeKind) -> Option<Self> {
        Some(match kind {
            PhysicalNodeKind::TopN(spec) => Self::TopN(spec),
            PhysicalNodeKind::Sort(spec) => Self::Sort(spec),
            PhysicalNodeKind::Aggregate(spec) => Self::Aggregate(spec),
            PhysicalNodeKind::SetOperation(spec) => Self::SetOperation(spec),
            PhysicalNodeKind::Window(spec) if !is_streaming_window_supported(spec) => {
                Self::Window(spec)
            }
            PhysicalNodeKind::PartitionAggregateWindow(spec) => {
                Self::PartitionAggregateWindow(spec)
            }
            PhysicalNodeKind::HashJoin(spec) => Self::HashJoin(spec),
            PhysicalNodeKind::NestedLoopJoin(spec) => Self::NestedLoopJoin(spec),
            PhysicalNodeKind::SortRangeJoin(spec) => Self::SortRangeJoin(spec),
            PhysicalNodeKind::ClassicIeJoin(spec) => Self::ClassicIeJoin(spec),
            PhysicalNodeKind::CrossProduct(spec) => Self::CrossProduct(spec),
            PhysicalNodeKind::ExternalTable(spec) => Self::ExternalTable(spec),
            PhysicalNodeKind::MaterializedCte(_) => Self::MaterializedCteControl,
            PhysicalNodeKind::DelimJoin(_) => Self::DelimJoinControl,
            PhysicalNodeKind::RecursiveCte(_) => Self::RecursiveCteControl,
            _ => return None,
        })
    }

    fn to_dispatch(&self) -> Option<BreakerDispatch> {
        Some(match self {
            Self::TopN(spec) => BreakerDispatch::TopN((*spec).clone()),
            Self::Sort(spec) => BreakerDispatch::Sort((*spec).clone()),
            Self::Aggregate(spec) => BreakerDispatch::Aggregate((*spec).clone()),
            Self::SetOperation(spec) => BreakerDispatch::SetOperation((*spec).clone()),
            Self::Window(spec) => BreakerDispatch::Window((*spec).clone()),
            Self::PartitionAggregateWindow(spec) => {
                BreakerDispatch::PartitionAggregateWindow((*spec).clone())
            }
            Self::HashJoin(spec) => BreakerDispatch::HashJoin((*spec).clone()),
            Self::NestedLoopJoin(spec) => BreakerDispatch::NestedLoopJoin((*spec).clone()),
            Self::SortRangeJoin(spec) => BreakerDispatch::SortRangeJoin((*spec).clone()),
            Self::ClassicIeJoin(spec) => BreakerDispatch::ClassicIeJoin((*spec).clone()),
            Self::CrossProduct(spec) => BreakerDispatch::CrossProduct((*spec).clone()),
            Self::ExternalTable(spec) => BreakerDispatch::ExternalTable((*spec).clone()),
            Self::MaterializedCteControl | Self::DelimJoinControl | Self::RecursiveCteControl => {
                return None
            }
        })
    }

    /// Clone only physical breakers that can actually construct an emit
    /// source. Scheduling eligibility is deliberately not decided here: the
    /// constructed `SourceSpec` is the sole authority for that contract.
    fn to_emit_dispatch(&self) -> Option<BreakerDispatch> {
        Some(match self {
            Self::TopN(spec) => BreakerDispatch::TopN((*spec).clone()),
            Self::Sort(spec) => BreakerDispatch::Sort((*spec).clone()),
            Self::Aggregate(spec) => BreakerDispatch::Aggregate((*spec).clone()),
            Self::SetOperation(spec) => BreakerDispatch::SetOperation((*spec).clone()),
            Self::Window(spec) => BreakerDispatch::Window((*spec).clone()),
            Self::PartitionAggregateWindow(spec) => {
                BreakerDispatch::PartitionAggregateWindow((*spec).clone())
            }
            Self::HashJoin(_)
            | Self::NestedLoopJoin(_)
            | Self::SortRangeJoin(_)
            | Self::ClassicIeJoin(_)
            | Self::CrossProduct(_)
            | Self::ExternalTable(_)
            | Self::MaterializedCteControl
            | Self::DelimJoinControl
            | Self::RecursiveCteControl => return None,
        })
    }

    fn is_tail_boundary(&self) -> bool {
        !matches!(self, Self::MaterializedCteControl)
    }
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
    fn from_dispatch(breaker: BreakerDispatch) -> Option<Self> {
        Some(match breaker {
            BreakerDispatch::Aggregate(spec) => Self::Aggregate(spec),
            BreakerDispatch::TopN(spec) => Self::TopN(spec),
            BreakerDispatch::Sort(spec) => Self::Sort(spec),
            BreakerDispatch::Window(spec) => Self::Window(spec),
            BreakerDispatch::PartitionAggregateWindow(spec) => Self::PartitionAggregateWindow(spec),
            BreakerDispatch::SetOperation(spec) => Self::SetOperation(spec),
            BreakerDispatch::HashJoin(_)
            | BreakerDispatch::NestedLoopJoin(_)
            | BreakerDispatch::SortRangeJoin(_)
            | BreakerDispatch::ClassicIeJoin(_)
            | BreakerDispatch::CrossProduct(_)
            | BreakerDispatch::ExternalTable(_) => return None,
        })
    }

    fn source(&self, handle: BreakerHandleId, output: &RowType) -> SourceSpec {
        match self {
            Self::Aggregate(spec) => aggregate_emit_source_spec(handle, spec.clone()),
            Self::TopN(spec) => SourceSpec::TopNEmit(TopNEmitSourceSpec {
                handle,
                spec: spec.clone(),
            }),
            Self::Sort(spec) => SourceSpec::SortEmit(SortEmitSourceSpec {
                handle,
                ordering: ordering_spec_from_orders(&spec.orders),
                output_names: output.names.clone(),
                output_types: output.types.clone(),
            }),
            Self::Window(spec) => SourceSpec::WindowEmit(WindowEmitSourceSpec {
                handle,
                spec: spec.clone(),
            }),
            Self::PartitionAggregateWindow(spec) => {
                SourceSpec::PartitionAggregateWindowEmit(PartitionAggregateWindowEmitSourceSpec {
                    handle,
                    spec: spec.clone(),
                })
            }
            Self::SetOperation(spec) => SourceSpec::SetOperationEmit(SetOperationEmitSourceSpec {
                handle,
                spec: spec.clone(),
            }),
        }
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
        let Some(build) = EmitBreakerBuild::from_dispatch(breaker) else {
            return Ok(None);
        };

        let handle = self
            .handles
            .register(build.handle_kind(), output.clone(), Default::default());
        let source = build.source(handle, &output);
        if require_parallel_probe && !source_supports_parallel_probe_fusion(&source) {
            self.handles.unregister_unbound(handle)?;
            return Ok(None);
        }
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
                    SinkSpec::TopNBuild(TopNBuildSinkSpec { handle, spec }),
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
                    SinkSpec::WindowBuild(WindowBuildSinkSpec { handle, spec }),
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
                        PartitionAggregateWindowBuildSinkSpec { handle, spec },
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
    /// the downstream probe chain. This is deliberately a static capability
    /// boundary rather than a row-count cost decision: even a small breaker
    /// must not silently cap an otherwise parallel probe pipeline.
    pub(crate) fn lower_breaker_to_probe_source(
        &mut self,
        root: PhysicalPlanNodeId,
        breaker: ProbeFusionCandidateDispatch,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<Option<BreakerProbeSource>> {
        self.lower_emit_breaker_source(root, breaker.0, true, pipelines, dependencies)
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
        BreakerRef::from_kind(&self.plan.node(root).kind)?.to_dispatch()
    }

    pub(crate) fn tail_breaker_dispatch(
        &self,
        root: PhysicalPlanNodeId,
    ) -> Result<BreakerDispatch> {
        BreakerRef::from_kind(&self.plan.node(root).kind)
            .and_then(|breaker| breaker.to_dispatch())
            .ok_or_else(|| paro_error::internal("tail breaker has no lowering dispatch"))
    }

    pub(crate) fn is_tail_breaker(kind: &PhysicalNodeKind) -> bool {
        BreakerRef::from_kind(kind).is_some_and(|breaker| breaker.is_tail_boundary())
    }

    pub(crate) fn probe_fusion_candidate_dispatch(
        kind: &PhysicalNodeKind,
    ) -> Option<ProbeFusionCandidateDispatch> {
        let breaker = BreakerRef::from_kind(kind)?;
        breaker.to_emit_dispatch().map(ProbeFusionCandidateDispatch)
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
