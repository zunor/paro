// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generic execution-row format contracts.
//!
//! Storage owns only the neutral protocol. Operator-specific layouts such as
//! hash join payload rows, sort run rows, and aggregate group rows live in the
//! execution crate so this module does not need to know join/sort/aggregate
//! semantics.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;

use crate::buffer::{BufferPool, MemoryTag};
use crate::row::{RowLayout, RowScanState, RowStore, RowStoreBuilder, RowValidityType};

/// Compile-time row format descriptor used by concrete spill/gather writers.
pub trait RowFormat: Clone + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn logical_types(&self) -> &[LogicalType];
}

/// Type-erased format metadata for manifests, EXPLAIN, and debug output.
///
/// This handle intentionally stores plain metadata instead of a trait object;
/// hot append/gather paths should use the concrete `F: RowFormat` type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFormatHandle {
    name: &'static str,
    logical_types: Box<[LogicalType]>,
}

impl RowFormatHandle {
    pub fn from_format<F: RowFormat>(format: &F) -> Self {
        Self {
            name: format.name(),
            logical_types: format.logical_types().to_vec().into_boxed_slice(),
        }
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    #[inline]
    pub fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }
}

/// Concrete spill writer protocol for one row format.
pub trait RowSpillWriter<F: RowFormat> {
    fn format(&self) -> &F;
    fn append_chunk(&mut self, input: &Chunk) -> Result<usize>;
    fn finish(self) -> Result<RowStore>;
}

/// Concrete spill reader protocol for one row format.
pub trait RowSpillReader<F: RowFormat> {
    fn format(&self) -> &F;
    fn read_next(&mut self, output: &mut Chunk) -> Result<usize>;
}

/// Concrete row-store writer for a known row format.
///
/// This is intentionally generic instead of boxed. Operators keep their
/// `F: RowFormat` in their own module, while storage owns the append/seal
/// protocol.
#[derive(Debug)]
pub struct RowStoreSpillWriter<F: RowFormat> {
    format: F,
    builder: RowStoreBuilder,
}

impl<F: RowFormat> RowStoreSpillWriter<F> {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        format: F,
        tag: MemoryTag,
        memory: MemoryAccountingContext,
    ) -> Self {
        let layout = Arc::new(RowLayout::from_types(
            format.logical_types().to_vec(),
            RowValidityType::CanHaveNullValues,
        ));
        Self {
            format,
            builder: RowStoreBuilder::new_with_memory(buffer_pool, layout, tag, memory),
        }
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.builder.count()
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.builder.size_in_bytes()
    }
}

impl<F: RowFormat> RowSpillWriter<F> for RowStoreSpillWriter<F> {
    fn format(&self) -> &F {
        &self.format
    }

    fn append_chunk(&mut self, input: &Chunk) -> Result<usize> {
        self.builder.append(input)
    }

    fn finish(self) -> Result<RowStore> {
        self.builder.try_seal()
    }
}

/// Concrete row-store reader for a known row format.
#[derive(Debug)]
pub struct RowStoreSpillReader<F: RowFormat> {
    format: F,
    store: RowStore,
    state: RowScanState,
}

impl<F: RowFormat> RowStoreSpillReader<F> {
    pub fn new(format: F, store: RowStore) -> Self {
        Self {
            format,
            store,
            state: RowScanState::default(),
        }
    }
}

impl<F: RowFormat> RowSpillReader<F> for RowStoreSpillReader<F> {
    fn format(&self) -> &F {
        &self.format
    }

    fn read_next(&mut self, output: &mut Chunk) -> Result<usize> {
        self.store.scan_with_state(&mut self.state, output)
    }
}
