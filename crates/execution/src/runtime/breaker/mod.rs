// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime-owned breaker handles and cleanup coordination.

pub mod adapter;
pub mod aggregate;
pub mod cleanup;
pub mod cte;
pub mod delim;
pub mod external_table;
pub mod join;
pub mod materialized;
pub mod partition_aggregate_window;
pub mod recursive;
pub mod registry;
pub mod set_operation;
pub mod shared_sink;
pub mod sort;
pub mod window;

pub use adapter::{
    MaterializeSinkExec, MaterializeSinkGlobal, MaterializeSinkLocal, MaterializedSourceExec,
    MaterializedSourceGlobal, MaterializedSourceLocal,
};
pub use aggregate::{
    single_state_addresses, AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer,
    AggregateHandle, AggregateLocalBuildCompactionReclaimer, AggregateLocalPayloadSpillReclaimer,
    AggregateLocalStateSpillReclaimer, AggregateRuntimeState, HashAggregateRuntimeState,
    PerfectHashAggregateRuntimeState, UngroupedAggregateRuntimeState,
};
pub use cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
pub use cte::CteHandle;
pub use delim::DelimHandle;
pub use external_table::ExternalTableHandle;
pub use join::{
    choose_hash_join_radix_bits, CompletionLatch, HashJoinBuildSpillReclaimer,
    HashJoinLocalBuildSpillReclaimer, JoinBuildHandle, JoinBuildId, JoinBuildMode,
    JoinBuildSpillBuffer, JoinBuildStats, JoinExternalModeConfig, JoinPartitionSet,
    JoinProbeSpillBuffer, JoinRuntimeFilterBuilder, JoinSpillState, JoinSpillStats, ProbeSpillSet,
};
pub use materialized::{FoundBits, MaterializedHandle, MaterializedReader};
pub use partition_aggregate_window::{
    PartitionAggregatePendingSpillReclaimer, PartitionAggregateWindowHandle,
};
pub use recursive::{RecursiveDedupSet, RecursiveTableHandle};
pub use registry::{
    BreakerHandleMetadata, BreakerHandleRegistry, HandleRef, RuntimeBreakerHandle,
    TypedBreakerHandle,
};
pub use set_operation::SetOperationHandle;
pub use shared_sink::{
    SharedSinkCoordinator, SharedSinkMergeEvent, SharedSinkProducerIndex, SharedSinkState,
};
pub use sort::{
    SortHandle, SortMaterializationBuild, SortOutputState, SortPendingRunsReclaimer,
    SortSealedState, TopNHandle, TopNRuntimeState,
};
pub use window::WindowHandle;
