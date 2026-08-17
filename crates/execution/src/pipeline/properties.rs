// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline-level physical property accumulation.

use crate::physical::properties::{
    ExecutionCapabilities, MemoryClass, MemoryRequirement, MorselCapability, MorselPartitioning,
    OrderingProperty, OrderingSpec, Parallelism, PartitioningProperty, PhysicalPropertySolver,
    PipelineProperties, Placement, PrefetchPolicy, PropertyRepair, PropertyRepairKind,
    ProvidedProperties,
};

use super::graph::{SinkSpec, SourceSpec, TransformSpec};

#[derive(Debug, Clone)]
pub struct PipelinePropertyBuild {
    /// Pipeline properties after every requested repair has been applied.
    pub properties: PipelineProperties,
    /// Lowering-time repair transforms that still need to be inserted into the
    /// pipeline shape.
    pub repair: PropertyRepair,
}

#[derive(Debug, Clone)]
pub struct PipelinePropertyAccumulator {
    current: ProvidedProperties,
    capabilities: ExecutionCapabilities,
    memory: MemoryRequirement,
    placement: Placement,
}

impl PipelinePropertyAccumulator {
    pub fn start_from_source(source: &SourceSpec) -> Self {
        let properties = source_properties(source);
        Self {
            current: properties.provided,
            capabilities: properties.capabilities,
            memory: properties.memory,
            placement: properties.placement,
        }
    }

    pub fn apply_transform(&mut self, transform: &TransformSpec) {
        match transform {
            TransformSpec::Filter(_)
            | TransformSpec::Project(_)
            | TransformSpec::HashJoinProbe(_)
            | TransformSpec::NestedLoopJoinProbe(_)
            | TransformSpec::SortRangeJoinProbe(_)
            | TransformSpec::CrossProductProbe(_)
            | TransformSpec::ExternalProject(_)
            | TransformSpec::GraphExpand(_)
            | TransformSpec::RowFetch(_)
            | TransformSpec::GraphProject(_) => {}
            TransformSpec::Limit(_)
            | TransformSpec::StreamingTopN(_)
            | TransformSpec::StreamingWindow(_)
            | TransformSpec::GraphShortestPath(_) => {
                self.capabilities.parallelism =
                    self.capabilities.parallelism.merge(Parallelism::single());
                self.placement = self.placement.merge(Placement::SingleTask);
                self.current.partitioning = PartitioningProperty::None;
                if matches!(transform, TransformSpec::StreamingTopN(_)) {
                    self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                }
            }
            TransformSpec::PropertyRepair(spec) => self.apply_repair(&spec.kind),
        }
    }

    pub fn close_with_sink(mut self, sink: &SinkSpec) -> PipelinePropertyBuild {
        match sink {
            SinkSpec::PerfectHashAggregate(spec) => {
                if let Some(plan) = spec.spec.perfect_hash.as_ref() {
                    self.capabilities.parallelism = self
                        .capabilities
                        .parallelism
                        .merge(Parallelism::bounded(plan.max_local_tables));
                }
            }
            SinkSpec::TopNBuild(_) => {
                // TopN retains its heap, normalized keys, and payload until
                // the emit boundary. It currently has neither a reclaimer nor
                // an external path, so it is blocking but not spillable.
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
            }
            SinkSpec::PartitionAggregateWindowBuild(_) => {
                // Local and merged payload/state can be atomically moved to
                // raw radix partitions. Finalization then works one bounded
                // partition at a time and publishes reclaiming output stores.
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                self.memory.revocable = true;
                self.memory.spillable = true;
                self.capabilities.supports_spill = true;
            }
            _ => {}
        }
        let required = sink.required_properties();
        let repair =
            PhysicalPropertySolver::reconcile(&required, &self.current, &self.capabilities);
        for kind in &repair.repairs {
            self.apply_repair(kind);
        }

        PipelinePropertyBuild {
            properties: PipelineProperties {
                placement: self.placement,
                required,
                provided: self.current,
                capabilities: self.capabilities,
                memory: self.memory,
                tuning: Default::default(),
            },
            repair,
        }
    }

    fn apply_repair(&mut self, repair: &PropertyRepairKind) {
        match repair {
            PropertyRepairKind::Sort(spec) => {
                self.current.ordering = OrderingProperty::Fixed(spec.clone());
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                self.memory.spillable = true;
            }
            PropertyRepairKind::BatchIndexAdapter => {
                self.current.partitioning = PartitioningProperty::BatchIndex;
            }
            PropertyRepairKind::SingleTaskFallback => {
                self.capabilities.parallelism = Parallelism::single();
                self.placement = self.placement.merge(Placement::SingleTask);
                if matches!(
                    self.current.ordering,
                    OrderingProperty::Any | OrderingProperty::Unordered
                ) {
                    // A single reader observes one stable source sequence. It
                    // does not manufacture a fixed sort order, but it does
                    // remove parallel batch interleaving.
                    self.current.ordering = OrderingProperty::Preserved;
                }
                self.current.partitioning = PartitioningProperty::None;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct SourceProperties {
    provided: ProvidedProperties,
    capabilities: ExecutionCapabilities,
    memory: MemoryRequirement,
    placement: Placement,
}

impl SourceProperties {
    fn validated(self) -> Self {
        debug_assert!(
            self.capabilities.parallelism.max == 1
                || !matches!(self.provided.ordering, OrderingProperty::Preserved),
            "a multi-task source cannot preserve one global consumption order"
        );
        debug_assert!(
            !matches!(self.provided.ordering, OrderingProperty::Unordered)
                || !self.capabilities.preserves_order,
            "an unordered source cannot advertise order-preserving execution"
        );
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmitSourceKind {
    HashAggregate,
    UngroupedAggregate,
    PerfectHashAggregate,
    TopN,
    Sort,
    Window,
    PartitionAggregateWindow,
    SetOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceScheduling {
    SingleTask,
    ParallelLocal,
    ParallelPartitioned(MorselPartitioning),
}

impl SourceScheduling {
    fn apply(self, properties: &mut SourceProperties) {
        match self {
            Self::SingleTask => {
                properties.provided.ordering = OrderingProperty::Preserved;
                properties.capabilities.parallelism = Parallelism::single();
                properties.capabilities.preserves_order = true;
                properties.placement = Placement::SingleTask;
            }
            Self::ParallelLocal => {
                properties.provided.ordering = OrderingProperty::Unordered;
                properties.capabilities.parallelism = Parallelism::unbounded();
                properties.capabilities.preserves_order = false;
                properties.placement = Placement::Local;
            }
            Self::ParallelPartitioned(partitioning) => {
                properties.provided.ordering = OrderingProperty::Unordered;
                properties.capabilities.parallelism = Parallelism::unbounded();
                properties.capabilities.preserves_order = false;
                properties.placement = Placement::Partitioned(partitioning);
            }
        }
    }
}

fn default_source_properties() -> SourceProperties {
    SourceProperties {
        provided: ProvidedProperties::default(),
        capabilities: ExecutionCapabilities::default(),
        memory: MemoryRequirement::default(),
        placement: Placement::Local,
    }
}

fn materialized_source_properties() -> SourceProperties {
    let mut properties = default_source_properties();
    SourceScheduling::ParallelLocal.apply(&mut properties);
    properties.memory.class = MemoryClass::Blocking;
    properties.validated()
}

fn emit_source_scheduling(kind: EmitSourceKind) -> SourceScheduling {
    match kind {
        EmitSourceKind::HashAggregate
        | EmitSourceKind::UngroupedAggregate
        | EmitSourceKind::PerfectHashAggregate
        | EmitSourceKind::PartitionAggregateWindow => SourceScheduling::ParallelLocal,
        EmitSourceKind::TopN
        | EmitSourceKind::Sort
        | EmitSourceKind::Window
        | EmitSourceKind::SetOperation => SourceScheduling::SingleTask,
    }
}

fn scheduling_properties(scheduling: SourceScheduling) -> SourceProperties {
    let mut properties = default_source_properties();
    scheduling.apply(&mut properties);
    properties.validated()
}

fn source_properties(source: &SourceSpec) -> SourceProperties {
    let mut properties = default_source_properties();

    let scheduling = match source {
        SourceSpec::Rowset(_) => {
            let partitioning = MorselPartitioning::rowset_segments();
            properties.provided.partitioning = PartitioningProperty::Morsel(partitioning);
            properties.capabilities.morsel = MorselCapability::Source;
            properties.capabilities.supports_late_materialization = true;
            SourceScheduling::ParallelPartitioned(partitioning)
        }
        SourceSpec::Values(_)
        | SourceSpec::Dummy(_)
        | SourceSpec::Empty(_)
        | SourceSpec::Chunk(_)
        | SourceSpec::Expression(_)
        | SourceSpec::TableFunction(_)
        | SourceSpec::VectorSearch(_)
        | SourceSpec::SparseVectorSearch(_)
        | SourceSpec::FullTextSearch(_)
        | SourceSpec::AdaptiveSearch(_) => SourceScheduling::SingleTask,
        SourceSpec::GraphScan(_) => SourceScheduling::ParallelLocal,
        SourceSpec::Materialized(_) => {
            properties.memory.class = MemoryClass::Blocking;
            SourceScheduling::ParallelLocal
        }
        SourceSpec::NljUnmatched(_)
        | SourceSpec::ClassicIeJoin(_)
        | SourceSpec::HashJoinSpillReplay(_)
        | SourceSpec::HashJoinUnmatched(_)
        | SourceSpec::CteScan(_)
        | SourceSpec::DelimScan(_)
        | SourceSpec::RecursiveTableScan(_)
        | SourceSpec::ExternalTable(_) => {
            properties.memory.class = MemoryClass::Blocking;
            SourceScheduling::ParallelLocal
        }
        SourceSpec::HashAggregateEmit(_) => {
            properties.memory.class = MemoryClass::Blocking;
            emit_source_scheduling(EmitSourceKind::HashAggregate)
        }
        SourceSpec::UngroupedAggregateEmit(_) => {
            properties.memory.class = MemoryClass::Blocking;
            emit_source_scheduling(EmitSourceKind::UngroupedAggregate)
        }
        SourceSpec::PerfectHashAggregateEmit(_) => {
            properties.memory.class = MemoryClass::Blocking;
            emit_source_scheduling(EmitSourceKind::PerfectHashAggregate)
        }
        SourceSpec::PartitionAggregateWindowEmit(_) => {
            properties.memory.class = MemoryClass::Blocking;
            emit_source_scheduling(EmitSourceKind::PartitionAggregateWindow)
        }
        SourceSpec::WindowEmit(_) => {
            properties.memory.class = MemoryClass::Blocking;
            emit_source_scheduling(EmitSourceKind::Window)
        }
        SourceSpec::SetOperationEmit(_) => {
            properties.memory.class = MemoryClass::Blocking;
            emit_source_scheduling(EmitSourceKind::SetOperation)
        }
        SourceSpec::SortEmit(spec) => {
            properties.memory.class = MemoryClass::Blocking;
            let scheduling = emit_source_scheduling(EmitSourceKind::Sort);
            scheduling.apply(&mut properties);
            properties.provided.ordering = OrderingProperty::Fixed(spec.ordering.clone());
            return properties.validated();
        }
        SourceSpec::TopNEmit(spec) => {
            properties.memory.class = MemoryClass::Blocking;
            let scheduling = emit_source_scheduling(EmitSourceKind::TopN);
            scheduling.apply(&mut properties);
            properties.provided.ordering =
                OrderingProperty::Fixed(ordering_spec_from_topn(&spec.spec.orders));
            return properties.validated();
        }
    };
    scheduling.apply(&mut properties);

    properties.validated()
}

/// Whether replacing a Materialized probe source with this native source
/// preserves the probe pipeline's ability to use every available worker.
///
/// Materialization is not merely a row-copy fallback: its readers are
/// unbounded and can restore parallelism after a single-task breaker emit.
/// Keep this decision next to the authoritative source property table so a
/// source's scheduling contract cannot drift from lowering eligibility.
pub(crate) fn emit_source_supports_parallel_probe_fusion(kind: EmitSourceKind) -> bool {
    let properties = scheduling_properties(emit_source_scheduling(kind));
    let materialized = materialized_source_properties();
    properties
        .capabilities
        .parallelism
        .dominates(materialized.capabilities.parallelism)
        && properties.placement.dominates(materialized.placement)
}

fn ordering_spec_from_topn(orders: &[paro_planner::binder::ir::OrderByNode]) -> OrderingSpec {
    let columns = orders
        .iter()
        .filter_map(|order| {
            let column = match &order.expression {
                paro_planner::expression::Expression::Reference(reference) => reference.index,
                paro_planner::expression::Expression::ColumnRef(column_ref) => {
                    column_ref.binding.column_index
                }
                _ => return None,
            };
            Some(crate::physical::properties::OrderingColumn {
                column,
                direction: if order.ascending {
                    crate::physical::properties::OrderingDirection::Asc
                } else {
                    crate::physical::properties::OrderingDirection::Desc
                },
                nulls: if order.nulls_first {
                    crate::physical::properties::NullOrdering::First
                } else {
                    crate::physical::properties::NullOrdering::Last
                },
            })
        })
        .collect();
    OrderingSpec::new(columns)
}

pub fn repair_transform(kind: PropertyRepairKind) -> TransformSpec {
    TransformSpec::PropertyRepair(super::graph::PropertyRepairSpec { kind })
}

pub fn rowset_prefetch_policy(distance: usize) -> PrefetchPolicy {
    PrefetchPolicy::RowsetSegments { distance }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::graph::MaterializedSourceSpec;
    use crate::pipeline::handles::BreakerHandleId;

    #[test]
    fn parallel_materialized_reader_is_explicitly_unordered() {
        let source = SourceSpec::Materialized(MaterializedSourceSpec {
            handle: BreakerHandleId::new(0),
        });
        let properties = source_properties(&source);
        assert_eq!(properties.provided.ordering, OrderingProperty::Unordered);
        assert_eq!(
            properties.capabilities.parallelism,
            Parallelism::unbounded()
        );
        assert!(!properties.capabilities.preserves_order);
    }

    #[test]
    fn every_parallel_scheduling_profile_is_explicitly_unordered() {
        for scheduling in [
            SourceScheduling::ParallelLocal,
            SourceScheduling::ParallelPartitioned(MorselPartitioning::rowset_segments()),
        ] {
            let properties = scheduling_properties(scheduling);
            assert_eq!(properties.provided.ordering, OrderingProperty::Unordered);
            assert_eq!(
                properties.capabilities.parallelism,
                Parallelism::unbounded()
            );
            assert!(!properties.capabilities.preserves_order);
        }
    }

    #[test]
    fn emit_probe_fusion_is_derived_from_emit_source_scheduling() {
        for kind in [
            EmitSourceKind::HashAggregate,
            EmitSourceKind::UngroupedAggregate,
            EmitSourceKind::PerfectHashAggregate,
            EmitSourceKind::PartitionAggregateWindow,
        ] {
            assert!(emit_source_supports_parallel_probe_fusion(kind));
        }
        for kind in [
            EmitSourceKind::TopN,
            EmitSourceKind::Sort,
            EmitSourceKind::Window,
            EmitSourceKind::SetOperation,
        ] {
            assert!(!emit_source_supports_parallel_probe_fusion(kind));
        }
    }

    #[test]
    fn single_task_repair_removes_parallel_interleaving() {
        let mut accumulator = PipelinePropertyAccumulator {
            current: ProvidedProperties {
                ordering: OrderingProperty::Unordered,
                ..Default::default()
            },
            capabilities: ExecutionCapabilities::default(),
            memory: MemoryRequirement::default(),
            placement: Placement::Local,
        };
        accumulator.apply_repair(&PropertyRepairKind::SingleTaskFallback);
        assert_eq!(accumulator.current.ordering, OrderingProperty::Preserved);
        assert_eq!(accumulator.capabilities.parallelism, Parallelism::single());
        assert_eq!(accumulator.placement, Placement::SingleTask);
    }
}
