// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{with_ordered_collection_maps, CatalogCollection, CatalogEntryMap};
use crate::entry::CatalogEntryEnum;
use crate::mvcc::VersionedEntry;
use paro_common::error::Result;
use std::sync::Arc;

#[derive(Debug)]
enum StagedCatalogMutationKind {
    Replace {
        key: String,
        head: Arc<VersionedEntry>,
        previous: Option<Arc<VersionedEntry>>,
    },
    Rename {
        old_key: String,
        old_head: Arc<VersionedEntry>,
        old_previous: Option<Arc<VersionedEntry>>,
        new_key: String,
        new_head: Arc<VersionedEntry>,
        new_previous: Option<Arc<VersionedEntry>>,
    },
    Move {
        source_key: String,
        source_head: Arc<VersionedEntry>,
        source_previous: Option<Arc<VersionedEntry>>,
        target_set: Arc<CatalogCollection>,
        target_key: String,
        target_head: Arc<VersionedEntry>,
        target_previous: Option<Arc<VersionedEntry>>,
    },
}

#[derive(Debug)]
pub struct StagedCatalogMutation {
    set: Arc<CatalogCollection>,
    kind: StagedCatalogMutationKind,
}

impl StagedCatalogMutation {
    pub fn publish(self, commit_id: u64) -> Result<()> {
        match &self.kind {
            StagedCatalogMutationKind::Replace { head, .. } => {
                head.set_timestamp(commit_id);
                if let Some(entry) = head.entry.as_ref() {
                    self.set.set_entry_timestamp(entry, commit_id);
                }
                self.set.mark_gc_dirty();
            }
            StagedCatalogMutationKind::Rename {
                old_head, new_head, ..
            } => {
                old_head.set_timestamp(commit_id);
                new_head.set_timestamp(commit_id);
                if let Some(entry) = new_head.entry.as_ref() {
                    self.set.set_entry_timestamp(entry, commit_id);
                }
                self.set.mark_gc_dirty();
            }
            StagedCatalogMutationKind::Move {
                source_head,
                target_set,
                target_head,
                ..
            } => {
                source_head.set_timestamp(commit_id);
                target_head.set_timestamp(commit_id);
                if let Some(entry) = target_head.entry.as_ref() {
                    target_set.set_entry_timestamp(entry, commit_id);
                }
                target_set.mark_gc_dirty();
            }
        }
        Ok(())
    }

    pub fn discard(self) -> Result<()> {
        match &self.kind {
            StagedCatalogMutationKind::Replace { key, previous, .. } => {
                let Ok(_write_lock) = self.set.catalog_lock.lock() else {
                    return Ok(());
                };
                let Ok(mut map) = self.set.map.write() else {
                    return Ok(());
                };
                Self::restore_previous(&mut map, key, previous);
                drop(map);
                drop(_write_lock);
                self.set.mark_gc_dirty();
            }
            StagedCatalogMutationKind::Rename {
                old_key,
                old_previous,
                new_key,
                new_previous,
                ..
            } => {
                let Ok(_write_lock) = self.set.catalog_lock.lock() else {
                    return Ok(());
                };
                let Ok(mut map) = self.set.map.write() else {
                    return Ok(());
                };
                Self::restore_previous(&mut map, old_key, old_previous);
                Self::restore_previous(&mut map, new_key, new_previous);
                drop(map);
                drop(_write_lock);
                self.set.mark_gc_dirty();
            }
            StagedCatalogMutationKind::Move {
                source_key,
                source_previous,
                target_set,
                target_key,
                target_previous,
                ..
            } => {
                with_ordered_collection_maps(&self.set, target_set, |source_map, target_map| {
                    Self::restore_previous(source_map, source_key, source_previous);
                    Self::restore_previous(target_map, target_key, target_previous);
                    Ok(())
                })?;
                target_set.mark_gc_dirty();
            }
        }
        Ok(())
    }

    pub fn entry(&self) -> Option<Arc<CatalogEntryEnum>> {
        match &self.kind {
            StagedCatalogMutationKind::Replace { head, .. } => head.entry.clone(),
            StagedCatalogMutationKind::Rename { new_head, .. } => new_head.entry.clone(),
            StagedCatalogMutationKind::Move { target_head, .. } => target_head.entry.clone(),
        }
    }

    pub(super) fn replace(
        set: Arc<CatalogCollection>,
        key: String,
        head: Arc<VersionedEntry>,
        previous: Option<Arc<VersionedEntry>>,
    ) -> Self {
        Self {
            set,
            kind: StagedCatalogMutationKind::Replace {
                key,
                head,
                previous,
            },
        }
    }

    pub(super) fn rename(
        set: Arc<CatalogCollection>,
        old_key: String,
        old_head: Arc<VersionedEntry>,
        old_previous: Option<Arc<VersionedEntry>>,
        new_key: String,
        new_head: Arc<VersionedEntry>,
        new_previous: Option<Arc<VersionedEntry>>,
    ) -> Self {
        Self {
            set,
            kind: StagedCatalogMutationKind::Rename {
                old_key,
                old_head,
                old_previous,
                new_key,
                new_head,
                new_previous,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn move_between_sets(
        source_set: Arc<CatalogCollection>,
        source_key: String,
        source_head: Arc<VersionedEntry>,
        source_previous: Option<Arc<VersionedEntry>>,
        target_set: Arc<CatalogCollection>,
        target_key: String,
        target_head: Arc<VersionedEntry>,
        target_previous: Option<Arc<VersionedEntry>>,
    ) -> Self {
        Self {
            set: source_set,
            kind: StagedCatalogMutationKind::Move {
                source_key,
                source_head,
                source_previous,
                target_set,
                target_key,
                target_head,
                target_previous,
            },
        }
    }

    fn restore_previous(
        map: &mut CatalogEntryMap,
        key: &str,
        previous: &Option<Arc<VersionedEntry>>,
    ) {
        match previous.as_ref() {
            Some(previous) => map.update_entry(key, previous.clone()),
            None => map.drop_entry(key),
        }
    }

    #[cfg(test)]
    pub fn commit(&self, commit_id: u64) {
        let cloned = Self {
            set: Arc::clone(&self.set),
            kind: match &self.kind {
                StagedCatalogMutationKind::Replace {
                    key,
                    head,
                    previous,
                } => StagedCatalogMutationKind::Replace {
                    key: key.clone(),
                    head: Arc::clone(head),
                    previous: previous.clone(),
                },
                StagedCatalogMutationKind::Rename {
                    old_key,
                    old_head,
                    old_previous,
                    new_key,
                    new_head,
                    new_previous,
                } => StagedCatalogMutationKind::Rename {
                    old_key: old_key.clone(),
                    old_head: Arc::clone(old_head),
                    old_previous: old_previous.clone(),
                    new_key: new_key.clone(),
                    new_head: Arc::clone(new_head),
                    new_previous: new_previous.clone(),
                },
                StagedCatalogMutationKind::Move {
                    source_key,
                    source_head,
                    source_previous,
                    target_set,
                    target_key,
                    target_head,
                    target_previous,
                } => StagedCatalogMutationKind::Move {
                    source_key: source_key.clone(),
                    source_head: Arc::clone(source_head),
                    source_previous: source_previous.clone(),
                    target_set: Arc::clone(target_set),
                    target_key: target_key.clone(),
                    target_head: Arc::clone(target_head),
                    target_previous: target_previous.clone(),
                },
            },
        };
        let _ = cloned.publish(commit_id);
    }

    #[cfg(test)]
    pub fn rollback(&self) {
        let cloned = Self {
            set: Arc::clone(&self.set),
            kind: match &self.kind {
                StagedCatalogMutationKind::Replace {
                    key,
                    head,
                    previous,
                } => StagedCatalogMutationKind::Replace {
                    key: key.clone(),
                    head: Arc::clone(head),
                    previous: previous.clone(),
                },
                StagedCatalogMutationKind::Rename {
                    old_key,
                    old_head,
                    old_previous,
                    new_key,
                    new_head,
                    new_previous,
                } => StagedCatalogMutationKind::Rename {
                    old_key: old_key.clone(),
                    old_head: Arc::clone(old_head),
                    old_previous: old_previous.clone(),
                    new_key: new_key.clone(),
                    new_head: Arc::clone(new_head),
                    new_previous: new_previous.clone(),
                },
                StagedCatalogMutationKind::Move {
                    source_key,
                    source_head,
                    source_previous,
                    target_set,
                    target_key,
                    target_head,
                    target_previous,
                } => StagedCatalogMutationKind::Move {
                    source_key: source_key.clone(),
                    source_head: Arc::clone(source_head),
                    source_previous: source_previous.clone(),
                    target_set: Arc::clone(target_set),
                    target_key: target_key.clone(),
                    target_head: Arc::clone(target_head),
                    target_previous: target_previous.clone(),
                },
            },
        };
        let _ = cloned.discard();
    }
}
