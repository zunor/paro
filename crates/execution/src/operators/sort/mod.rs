// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};

use crate::runtime::context::QueryRuntimeContext;

pub mod build;
pub(crate) mod emit;
mod finalize;
pub mod row_format;
pub mod state;
pub(crate) mod streaming_topn;
pub mod topn_build;
pub(crate) mod topn_emit;
pub mod topn_heap;

pub use build::SortBuildSinkExec;
pub use emit::SortEmitSourceExec;
pub use streaming_topn::StreamingTopNTransformExec;
pub use topn_build::TopNBuildSinkExec;
pub use topn_emit::TopNEmitSourceExec;

/// Query-owned accounting domain shared by blocking and streaming TopN.
pub(crate) fn topn_memory_context(query: &QueryRuntimeContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = query.memory.clone();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        // TopN has no reclaimer: its heap, sort keys, and retained payloads
        // remain live until extraction. Publishing them as revocable would
        // overstate reclaimable query memory and make pressure recovery fail.
        MemoryAccountingClass::NonRevocable,
    )
}
