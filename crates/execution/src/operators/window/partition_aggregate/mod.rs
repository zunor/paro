// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sort-free full-partition aggregate window.

mod build;
mod emit;
mod index;
mod state;

use parking_lot::Mutex;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_storage::row::{RowFormat, RowStore};

use crate::memory_runtime::RetainedMemoryHandle;
use crate::operators::aggregate::payload_spill::AggregateSpilledPayload;
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;

pub use build::PartitionAggregateWindowBuildSinkExec;
pub use emit::PartitionAggregateWindowEmitSourceExec;
pub(crate) use index::FinalizedPartitionIndex;
pub use state::{PartitionAggregateEmitGlobal, PartitionAggregateEmitLocal};

/// One sink-local contribution after pipeline merge.
#[derive(Debug)]
pub(crate) enum PartitionAggregateLocalOutput {
    Columnar {
        payloads: Vec<Chunk>,
        tables: Vec<AggregateHashTable>,
        payload_memory: Arc<RetainedMemoryHandle>,
    },
    External(AggregateSpilledPayload),
}

#[derive(Debug)]
pub(crate) enum PartitionAggregateSnapshot {
    /// Zero-key aggregate result paired with detail-projected batches. Both
    /// in-memory and external payloads use this exact output-order layout.
    /// Every emitted row observes the same finalized aggregate values, so no
    /// lookup index or per-row selection vector exists in this domain.
    Global {
        payloads: Arc<[Chunk]>,
        /// Detail batches that crossed the query memory boundary are owned by
        /// exactly one emit task, just like keyed external output partitions.
        external_payloads: Mutex<Vec<Option<RowStore>>>,
        aggregates: Box<[Value]>,
        _payload_memory: Box<[Arc<RetainedMemoryHandle>]>,
        spilled_bytes: usize,
    },
    InMemory {
        payloads: Arc<[Chunk]>,
        index: FinalizedPartitionIndex,
        /// Leases transferred continuously from sink-local retention.
        _payload_memory: Box<[Arc<RetainedMemoryHandle>]>,
    },
    External {
        /// Each output partition has exactly one owner after `take_output`.
        /// A task converts that store to a reclaiming cursor and frees prefixes
        /// while producing rows.
        outputs: Mutex<Vec<Option<RowStore>>>,
        /// Final raw payload directory plus finalized output stores. Spill is
        /// observational and never consumes the query working-memory grant.
        spilled_bytes: usize,
        /// Number of radix attempts, including the initial partitioning.
        repartition_depth: usize,
    },
}

impl PartitionAggregateSnapshot {
    pub(crate) fn work_count(&self) -> usize {
        match self {
            Self::Global {
                payloads,
                external_payloads,
                ..
            } => payloads
                .len()
                .saturating_add(external_payloads.lock().len()),
            Self::InMemory { payloads, .. } => payloads.len(),
            Self::External { outputs, .. } => outputs.lock().len(),
        }
    }

    pub(crate) fn global_batch(&self, index: usize) -> Option<(&Chunk, &[Value])> {
        let Self::Global {
            payloads,
            aggregates,
            ..
        } = self
        else {
            return None;
        };
        payloads
            .get(index)
            .map(|payload| (payload, aggregates.as_ref()))
    }

    pub(crate) fn global_aggregates(&self) -> Option<&[Value]> {
        let Self::Global { aggregates, .. } = self else {
            return None;
        };
        Some(aggregates)
    }

    pub(crate) fn take_global_external_payload(&self, index: usize) -> Option<RowStore> {
        let Self::Global {
            payloads,
            external_payloads,
            ..
        } = self
        else {
            return None;
        };
        external_payloads
            .lock()
            .get_mut(index.checked_sub(payloads.len())?)?
            .take()
    }

    pub(crate) fn take_output(&self, index: usize) -> Option<RowStore> {
        let Self::External { outputs, .. } = self else {
            return None;
        };
        outputs.lock().get_mut(index)?.take()
    }

    pub(crate) fn in_memory_batch(
        &self,
        index: usize,
    ) -> Option<(&Chunk, &FinalizedPartitionIndex)> {
        let Self::InMemory {
            payloads,
            index: lookup,
            ..
        } = self
        else {
            return None;
        };
        payloads.get(index).map(|payload| (payload, lookup))
    }

    pub(crate) fn is_external(&self) -> bool {
        matches!(self, Self::External { .. })
    }

    pub(crate) fn spill_stats(&self) -> Option<(usize, usize)> {
        match self {
            Self::External {
                spilled_bytes,
                repartition_depth,
                ..
            } => Some((*spilled_bytes, *repartition_depth)),
            Self::Global { spilled_bytes, .. } => {
                (*spilled_bytes > 0).then_some((*spilled_bytes, 0))
            }
            Self::InMemory { .. } => None,
        }
    }

    pub(crate) fn set_external_spill_stats(
        &mut self,
        input_spilled_bytes: usize,
        repartition_depth: usize,
    ) {
        let Self::External {
            outputs,
            spilled_bytes,
            repartition_depth: depth,
        } = self
        else {
            return;
        };
        *spilled_bytes = input_spilled_bytes.saturating_add(
            outputs
                .get_mut()
                .iter()
                .filter_map(Option::as_ref)
                .map(RowStore::size_in_bytes)
                .sum::<usize>(),
        );
        *depth = repartition_depth;
    }
}

/// Spill layout for the detail side of a zero-key aggregate window. Aggregate
/// state is finalized once in memory; only the replay payload crosses this
/// row-store boundary.
#[derive(Debug, Clone)]
pub(crate) struct GlobalPartitionAggregateDetailFormat {
    detail_types: Box<[LogicalType]>,
}

impl GlobalPartitionAggregateDetailFormat {
    pub(crate) fn new(detail_types: &[LogicalType]) -> Self {
        Self {
            detail_types: detail_types.to_vec().into_boxed_slice(),
        }
    }
}

impl RowFormat for GlobalPartitionAggregateDetailFormat {
    fn name(&self) -> &'static str {
        "global_partition_aggregate_detail"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.detail_types
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionAggregateOutputFormat {
    output_types: Box<[LogicalType]>,
}

impl PartitionAggregateOutputFormat {
    pub(crate) fn new(output_types: &[LogicalType]) -> Self {
        Self {
            output_types: output_types.to_vec().into_boxed_slice(),
        }
    }
}

impl RowFormat for PartitionAggregateOutputFormat {
    fn name(&self) -> &'static str {
        "partition_aggregate_window_output"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.output_types
    }
}
