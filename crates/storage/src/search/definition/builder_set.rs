// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::index::fulltext::tokenizer::TokenizerKind;
use paro_common::error::Result;
use serde_json::Value;

use crate::search::capability::{SearchIndexDefinition, SearchIndexKind};
use crate::search::inline_sink::{
    FlushSearchMode, HnswSegmentInlineArtifactBuilder, SearchAdmission, SearchInlineBuilderEntry,
    SearchInlineBuilderSet,
};
use crate::search::providers::fulltext::inline::FullTextInlineArtifactBuilder;
use crate::search::providers::sparse::inline::SparseInlineArtifactBuilder;
use crate::search::stats::SearchGenerationId;
use crate::tablet::ColumnId;

pub(crate) fn build_inline_builder_set<'a>(
    definitions: impl IntoIterator<Item = (&'a SearchIndexDefinition, SearchGenerationId)>,
    admission: Option<Arc<dyn SearchAdmission>>,
) -> Result<SearchInlineBuilderSet> {
    let mut entries = BTreeMap::<InlineBuilderPhysicalKey, SearchInlineBuilderEntry>::new();
    for (definition, generation_id) in definitions {
        let key = inline_builder_physical_key(definition)?;
        let entry = match definition.kind {
            SearchIndexKind::FullText => SearchInlineBuilderEntry::new(
                definition.clone(),
                generation_id,
                definition.freshness_policy,
                Arc::new(FullTextInlineArtifactBuilder),
            ),
            SearchIndexKind::Sparse => SearchInlineBuilderEntry::new(
                definition.clone(),
                generation_id,
                definition.freshness_policy,
                Arc::new(SparseInlineArtifactBuilder),
            ),
            SearchIndexKind::Hnsw => SearchInlineBuilderEntry::new(
                definition.clone(),
                generation_id,
                definition.freshness_policy,
                Arc::new(HnswSegmentInlineArtifactBuilder),
            ),
        };
        match entries.entry(key) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(entry);
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                if flush_mode_rank(entry.flush_mode()) > flush_mode_rank(slot.get().flush_mode()) {
                    slot.insert(entry);
                }
            }
        }
    }
    Ok(SearchInlineBuilderSet::new(
        entries.into_values().collect(),
        admission,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InlineBuilderPhysicalKey {
    kind: SearchIndexKind,
    column_ids: Vec<ColumnId>,
    config_key: String,
}

fn inline_builder_physical_key(
    definition: &SearchIndexDefinition,
) -> Result<InlineBuilderPhysicalKey> {
    let config_key = match definition.kind {
        SearchIndexKind::FullText => {
            let config = definition
                .provider_config
                .get("config")
                .and_then(Value::as_str)
                .unwrap_or("simple");
            TokenizerKind::from_config(config)?
                .config_name()
                .to_string()
        }
        SearchIndexKind::Sparse => String::new(),
        SearchIndexKind::Hnsw => definition.config_fingerprint.to_string(),
    };
    Ok(InlineBuilderPhysicalKey {
        kind: definition.kind,
        column_ids: definition.column_ids.clone(),
        config_key,
    })
}

const fn flush_mode_rank(mode: FlushSearchMode) -> u8 {
    match mode {
        FlushSearchMode::TailOnly => 0,
        FlushSearchMode::InlineIfAdmitted => 1,
        FlushSearchMode::InlineRequired => 2,
    }
}
