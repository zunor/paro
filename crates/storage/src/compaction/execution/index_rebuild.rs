// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::types::CompactionPlan;
use crate::rowset::RowsetSharedPtr;
use crate::search::manifest::ManifestStore;
use crate::search::{SearchGenerationId, SearchIndexDefinition, SearchInlineBuilderSet};
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSearchGeneration {
    pub definition_id: u64,
    pub generation_id: SearchGenerationId,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CompactionGenerationContext {
    pub active_generations: Vec<ActiveSearchGeneration>,
    /// Immutable definition snapshot used to write this compaction output.
    /// Provider rebuilders must derive physical contracts from this source,
    /// never from lossy schema compatibility fields or source artifacts.
    pub search_definitions: Vec<SearchIndexDefinition>,
}

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
        _generation_context: &CompactionGenerationContext,
        _tablet: &Tablet,
        _rowset: &RowsetSharedPtr,
        _plan: &CompactionPlan,
    ) -> bool {
        true
    }

    /// Rebuild indexes for the given compaction output rowset.
    fn rebuild(
        &self,
        generation_context: &CompactionGenerationContext,
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
    search_inline_builders: &SearchInlineBuilderSet,
) -> Result<()> {
    ensure_default_rebuilders_registered();
    let generation_context = load_generation_context(tablet, search_inline_builders)?;

    let builders = CompactionIndexRegistry::global().list()?;
    if builders.is_empty() {
        return Ok(());
    }

    let applicable: Vec<_> = builders
        .into_iter()
        .filter(|b| b.is_applicable(&generation_context, tablet, &rowset, plan))
        .collect();

    if applicable.is_empty() {
        return Ok(());
    }

    rowset.load()?;

    if applicable.len() == 1 {
        return applicable[0].rebuild(&generation_context, tablet, &rowset, plan);
    }

    let mut errors = Vec::new();
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(applicable.len());
        for builder in applicable {
            let rowset = rowset.clone();
            let generation_context = generation_context.clone();
            handles.push(
                scope.spawn(move || builder.rebuild(&generation_context, tablet, &rowset, plan)),
            );
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

fn load_generation_context(
    tablet: &Tablet,
    search_inline_builders: &SearchInlineBuilderSet,
) -> Result<CompactionGenerationContext> {
    let manifests = ManifestStore::new(tablet.data_dir().to_path_buf());
    let mut active_generations = Vec::new();
    for head in tablet.search_generation_heads() {
        if let Some(manifest) = manifests.load_manifest_for_head(&head)? {
            active_generations.push(ActiveSearchGeneration {
                definition_id: head.definition_id,
                generation_id: manifest.root.generation_id,
            });
        }
    }
    active_generations.sort_by_key(|entry| entry.definition_id);
    let mut search_definitions = search_inline_builders
        .entries()
        .iter()
        .map(|entry| entry.definition.clone())
        .collect::<Vec<_>>();
    search_definitions.sort_by_key(|definition| definition.definition_id);
    Ok(CompactionGenerationContext {
        active_generations,
        search_definitions,
    })
}

#[cfg(test)]
pub fn clear_compaction_index_rebuilders_for_tests() {
    CompactionIndexRegistry::global().clear();
}
