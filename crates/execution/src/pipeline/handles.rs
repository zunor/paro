// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Breaker handle catalog for pipeline lowering.
//!
//! The catalog is metadata only. Runtime handles, hash tables, spill files and
//! memory grants are created later by the runtime handle registry.

use paro_common::error::{self as paro_error, Result};

use crate::physical::properties::PipelineProperties;
use crate::physical::row_type::RowType;

use super::graph::PipelineId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BreakerHandleId(u32);

impl BreakerHandleId {
    pub fn new(index: usize) -> Self {
        assert!(
            index <= u32::MAX as usize,
            "breaker handle catalog exhausted"
        );
        Self(index as u32)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerHandleKind {
    Materialized,
    Sort,
    TopN,
    HashJoinBuild,
    Aggregate,
    Window,
    PartitionAggregateWindow,
    SetOperation,
    Cte,
    Delim,
    RecursiveTable,
    ExternalTable,
}

#[derive(Debug, Clone)]
pub struct BreakerHandleEntry {
    pub id: BreakerHandleId,
    pub kind: BreakerHandleKind,
    pub row_type: RowType,
    pub producer: Option<PipelineId>,
    pub consumers: Vec<PipelineId>,
    pub properties: PipelineProperties,
}

#[derive(Debug, Clone, Default)]
pub struct BreakerHandleCatalog {
    entries: Vec<BreakerHandleEntry>,
}

impl BreakerHandleCatalog {
    pub fn get(&self, id: BreakerHandleId) -> Option<&BreakerHandleEntry> {
        self.entries.get(id.index())
    }

    pub fn iter(&self) -> impl Iterator<Item = &BreakerHandleEntry> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        for (idx, entry) in self.entries.iter().enumerate() {
            if entry.id.index() != idx {
                return Err(paro_error::internal(format!(
                    "breaker handle catalog id mismatch at {idx}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BreakerHandleCatalogBuilder {
    entries: Vec<BreakerHandleEntry>,
}

impl BreakerHandleCatalogBuilder {
    pub fn register(
        &mut self,
        kind: BreakerHandleKind,
        row_type: RowType,
        properties: PipelineProperties,
    ) -> BreakerHandleId {
        let id = BreakerHandleId::new(self.entries.len());
        self.entries.push(BreakerHandleEntry {
            id,
            kind,
            row_type,
            producer: None,
            consumers: Vec::new(),
            properties,
        });
        id
    }

    pub fn set_producer(&mut self, id: BreakerHandleId, producer: PipelineId) -> Result<()> {
        let entry = self
            .entries
            .get_mut(id.index())
            .ok_or_else(|| paro_error::internal("unknown breaker handle id"))?;
        entry.producer = Some(producer);
        Ok(())
    }

    pub fn add_consumer(&mut self, id: BreakerHandleId, consumer: PipelineId) -> Result<()> {
        let entry = self
            .entries
            .get_mut(id.index())
            .ok_or_else(|| paro_error::internal("unknown breaker handle id"))?;
        if !entry.consumers.contains(&consumer) {
            entry.consumers.push(consumer);
        }
        Ok(())
    }

    pub fn finish(self) -> BreakerHandleCatalog {
        BreakerHandleCatalog {
            entries: self.entries,
        }
    }
}
