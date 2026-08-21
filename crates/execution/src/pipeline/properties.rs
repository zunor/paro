// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline scheduling and memory contracts.
//!
//! Only properties consumed by the scheduler or memory subsystem live here.
//! Logical ordering and partitioning requirements must be lowered into real
//! physical operators before pipeline construction, not into inert adapter
//! transforms.

use crate::physical::properties::{
    ExecutionCapabilities, MemoryClass, MemoryRequirement, MorselCapability, Parallelism,
    PipelineProperties, PrefetchPolicy,
};

use super::graph::{SinkSpec, SourceSpec, TransformSpec};

#[derive(Debug, Clone)]
pub struct PipelinePropertyAccumulator {
    capabilities: ExecutionCapabilities,
    memory: MemoryRequirement,
}

impl PipelinePropertyAccumulator {
    pub fn start_from_source(source: &SourceSpec) -> Self {
        let properties = source_properties(source);
        Self {
            capabilities: properties.capabilities,
            memory: properties.memory,
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
                if matches!(transform, TransformSpec::StreamingTopN(_)) {
                    self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                }
            }
        }
    }

    pub fn close_with_sink(mut self, sink: &SinkSpec) -> PipelineProperties {
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
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
            }
            SinkSpec::PartitionAggregateWindowBuild(_) => {
                self.memory.class = self.memory.class.max(MemoryClass::Blocking);
                self.memory.revocable = true;
                self.memory.spillable = true;
                self.capabilities.supports_spill = true;
            }
            _ => {}
        }

        PipelineProperties {
            capabilities: self.capabilities,
            memory: self.memory,
            tuning: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct SourceProperties {
    capabilities: ExecutionCapabilities,
    memory: MemoryRequirement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceClass {
    SingleTask,
    Parallel,
    ParallelBlocking,
    Rowset,
    ParallelEmit,
    SingleTaskEmit,
}

impl SourceSpec {
    /// Authoritative source classification. Both scheduling properties and
    /// breaker-probe fusion consume this exact classification.
    fn property_class(&self) -> SourceClass {
        match self {
            Self::Values(_)
            | Self::Dummy(_)
            | Self::Empty(_)
            | Self::Chunk(_)
            | Self::Expression(_)
            | Self::TableFunction(_)
            | Self::VectorSearch(_)
            | Self::SparseVectorSearch(_)
            | Self::FullTextSearch(_)
            | Self::AdaptiveSearch(_) => SourceClass::SingleTask,
            Self::Rowset(_) => SourceClass::Rowset,
            Self::GraphScan(_) => SourceClass::Parallel,
            Self::Materialized(_)
            | Self::NljUnmatched(_)
            | Self::ClassicIeJoin(_)
            | Self::HashJoinSpillReplay(_)
            | Self::HashJoinUnmatched(_)
            | Self::CteScan(_)
            | Self::DelimScan(_)
            | Self::RecursiveTableScan(_)
            | Self::ExternalTable(_) => SourceClass::ParallelBlocking,
            Self::HashAggregateEmit(_)
            | Self::UngroupedAggregateEmit(_)
            | Self::PerfectHashAggregateEmit(_)
            | Self::PartitionAggregateWindowEmit(_) => SourceClass::ParallelEmit,
            Self::TopNEmit(_)
            | Self::SortEmit(_)
            | Self::WindowEmit(_)
            | Self::SetOperationEmit(_) => SourceClass::SingleTaskEmit,
        }
    }

    fn is_emit_source(&self) -> bool {
        matches!(
            self.property_class(),
            SourceClass::ParallelEmit | SourceClass::SingleTaskEmit
        )
    }
}

fn source_class_parallelism(class: SourceClass) -> Parallelism {
    match class {
        SourceClass::SingleTask | SourceClass::SingleTaskEmit => Parallelism::single(),
        SourceClass::Parallel
        | SourceClass::ParallelBlocking
        | SourceClass::Rowset
        | SourceClass::ParallelEmit => Parallelism::unbounded(),
    }
}

fn source_properties(source: &SourceSpec) -> SourceProperties {
    let class = source.property_class();
    let mut capabilities = ExecutionCapabilities {
        parallelism: source_class_parallelism(class),
        ..ExecutionCapabilities::default()
    };
    let mut memory = MemoryRequirement::default();

    match class {
        SourceClass::SingleTask | SourceClass::Parallel => {}
        SourceClass::ParallelBlocking => {
            memory.class = MemoryClass::Blocking;
        }
        SourceClass::Rowset => {
            capabilities.morsel = MorselCapability::Source;
            capabilities.supports_late_materialization = true;
        }
        SourceClass::ParallelEmit | SourceClass::SingleTaskEmit => {
            memory.class = MemoryClass::Blocking;
        }
    }

    SourceProperties {
        capabilities,
        memory,
    }
}

fn materialized_source_parallelism() -> Parallelism {
    source_class_parallelism(SourceClass::ParallelBlocking)
}

/// Final probe-fusion eligibility check over the source that will execute.
pub(crate) fn source_supports_parallel_probe_fusion(source: &SourceSpec) -> bool {
    source.is_emit_source()
        && source_properties(source)
            .capabilities
            .parallelism
            .dominates(materialized_source_parallelism())
}

pub fn rowset_prefetch_policy(distance: usize) -> PrefetchPolicy {
    PrefetchPolicy::RowsetSegments { distance }
}
