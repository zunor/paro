// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join memory accounting helpers.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryDomain};

use crate::runtime::context::QueryRuntimeContext;

pub(crate) fn hash_join_memory_context(query: &QueryRuntimeContext) -> MemoryAccountingContext {
    hash_join_memory_context_with_class(query, MemoryAccountingClass::Revocable)
}

pub(crate) fn hash_join_spill_memory_context(
    query: &QueryRuntimeContext,
) -> MemoryAccountingContext {
    hash_join_memory_context_with_class(query, MemoryAccountingClass::Spill)
}

fn hash_join_memory_context_with_class(
    query: &QueryRuntimeContext,
    class: MemoryAccountingClass,
) -> MemoryAccountingContext {
    let owner: Arc<dyn paro_common::memory::MemoryOwner> = query.memory.clone();
    MemoryAccountingContext::from_owner(owner, MemoryDomain::Host, MemoryTag::HashTable, class)
}
