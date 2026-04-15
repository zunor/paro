// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::types::CompactionPlan;
use crate::rowset::RowsetSharedPtr;
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};
use std::sync::{Arc, OnceLock, RwLock};

/// Trait for rebuilding secondary indexes during compaction.
///
/// This is a lightweight extension point for vector/full-text indexes
/// to rebuild against a newly produced rowset.
pub trait CompactionIndexRebuilder: Send + Sync {
    /// Human-readable name for logging/debugging.
    fn name(&self) -> &'static str;

    /// Whether this rebuilder should run for the given compaction output.
    fn is_applicable(
        &self,
        _tablet: &Tablet,
        _rowset: &RowsetSharedPtr,
        _plan: &CompactionPlan,
    ) -> bool {
        true
    }

    /// Rebuild indexes for the given compaction output rowset.
    fn rebuild(
        &self,
        tablet: &Tablet,
        rowset: &RowsetSharedPtr,
        plan: &CompactionPlan,
    ) -> Result<()>;
}

struct CompactionIndexRegistry {
    builders: RwLock<Vec<Arc<dyn CompactionIndexRebuilder>>>,
}

impl CompactionIndexRegistry {
    fn global() -> &'static CompactionIndexRegistry {
        static REGISTRY: OnceLock<CompactionIndexRegistry> = OnceLock::new();
        REGISTRY.get_or_init(|| CompactionIndexRegistry {
            builders: RwLock::new(Vec::new()),
        })
    }

    fn register(&self, builder: Arc<dyn CompactionIndexRebuilder>) -> Result<()> {
        let mut builders = self
            .builders
            .write()
            .map_err(|_| paro_error::internal("Compaction index registry poisoned"))?;

        if builders.iter().any(|b| b.name() == builder.name()) {
            return Err(paro_error::object_exists(
                "compaction index rebuilder",
                builder.name(),
            ));
        }

        builders.push(builder);
        Ok(())
    }

    fn list(&self) -> Result<Vec<Arc<dyn CompactionIndexRebuilder>>> {
        let builders = self
            .builders
            .read()
            .map_err(|_| paro_error::internal("Compaction index registry poisoned"))?;
        Ok(builders.clone())
    }

    #[cfg(test)]
    fn clear(&self) {
        if let Ok(mut builders) = self.builders.write() {
            builders.clear();
        }
    }
}

fn ensure_default_rebuilders_registered() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        // Best-effort registration; ignore duplicate errors.
        let _ = crate::index::art::compaction::register_art_rebuilder();
        let _ = crate::index::hnsw::compaction::register_hnsw_rebuilder();
        let _ = crate::index::fulltext::compaction::register_fulltext_rebuilder();
    });
}

/// Register a compaction index rebuilder (e.g., HNSW/FTS rebuilders).
pub fn register_compaction_index_rebuilder(
    builder: Arc<dyn CompactionIndexRebuilder>,
) -> Result<()> {
    CompactionIndexRegistry::global().register(builder)
}

/// Run all registered rebuilders for the given compaction output rowset.
///
/// Rebuilders are executed in parallel when more than one is applicable.
pub fn rebuild_compaction_indexes(
    tablet: &Tablet,
    rowset: RowsetSharedPtr,
    plan: &CompactionPlan,
) -> Result<()> {
    ensure_default_rebuilders_registered();

    let builders = CompactionIndexRegistry::global().list()?;
    if builders.is_empty() {
        return Ok(());
    }

    let applicable: Vec<_> = builders
        .into_iter()
        .filter(|b| b.is_applicable(tablet, &rowset, plan))
        .collect();

    if applicable.is_empty() {
        return Ok(());
    }

    rowset.load()?;

    if applicable.len() == 1 {
        return applicable[0].rebuild(tablet, &rowset, plan);
    }

    let mut errors = Vec::new();
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(applicable.len());
        for builder in applicable {
            let rowset = rowset.clone();
            handles.push(scope.spawn(move || builder.rebuild(tablet, &rowset, plan)));
        }

        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => errors.push(err),
                Err(_) => errors.push(paro_error::internal("Compaction index rebuilder panicked")),
            }
        }
    });

    if let Some(err) = errors.into_iter().next() {
        return Err(err);
    }

    Ok(())
}

#[cfg(test)]
pub fn clear_compaction_index_rebuilders_for_tests() {
    CompactionIndexRegistry::global().clear();
}
