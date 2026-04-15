use super::CatalogCollection;
use crate::mvcc::{self, VersionedEntry};
use std::sync::Arc;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CatalogGcStats {
    pub chains_scanned: usize,
    pub chains_rebuilt: usize,
    pub chains_skipped_provisional: usize,
    pub chains_skipped_trivial: usize,
    pub nodes_scanned: usize,
    pub nodes_pruned: usize,
}

impl CatalogGcStats {
    pub fn merge(&mut self, other: &CatalogGcStats) {
        self.chains_scanned += other.chains_scanned;
        self.chains_rebuilt += other.chains_rebuilt;
        self.chains_skipped_provisional += other.chains_skipped_provisional;
        self.chains_skipped_trivial += other.chains_skipped_trivial;
        self.nodes_scanned += other.nodes_scanned;
        self.nodes_pruned += other.nodes_pruned;
    }

    pub fn chains_skipped(&self) -> usize {
        self.chains_skipped_provisional + self.chains_skipped_trivial
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CatalogReplaySummary {
    pub max_catalog_commit_id: u64,
    pub max_seen_object_id: u64,
}

pub(super) fn run_collection_gc(set: &CatalogCollection, watermark: u64) -> CatalogGcStats {
    let mut stats = CatalogGcStats::default();

    let Ok(_catalog_lock) = set.catalog_lock.lock() else {
        return stats;
    };
    let Ok(mut map) = set.map.write() else {
        return stats;
    };

    let heads = map
        .entries()
        .map(|(key, head)| (key.clone(), Arc::clone(head)))
        .collect::<Vec<_>>();
    let mut rebuilt_heads = Vec::new();

    for (key, head) in heads {
        stats.chains_scanned += 1;

        let head_timestamp = head.timestamp();
        if mvcc::is_provisional(head_timestamp) {
            stats.chains_skipped_provisional += 1;
            stats.nodes_scanned += 1;
            continue;
        }

        if head.child().is_none()
            && (mvcc::is_permanent(head_timestamp) || mvcc::is_committed(head_timestamp))
        {
            stats.chains_skipped_trivial += 1;
            stats.nodes_scanned += 1;
            continue;
        }

        let mut original = Vec::new();
        let mut retained = Vec::new();
        let mut retained_anchor = false;
        let mut current = Some(head);
        while let Some(node) = current {
            stats.nodes_scanned += 1;
            let timestamp = node.timestamp();
            let keep = if mvcc::is_permanent(timestamp)
                || mvcc::is_provisional(timestamp)
                || timestamp >= watermark
            {
                true
            } else if !retained_anchor {
                retained_anchor = true;
                true
            } else {
                false
            };
            if keep {
                retained.push(Arc::clone(&node));
            }
            original.push(Arc::clone(&node));
            current = node.child();
        }

        if retained.len() == original.len() {
            continue;
        }

        stats.chains_rebuilt += 1;
        stats.nodes_pruned += original.len().saturating_sub(retained.len());
        let new_head = retained
            .iter()
            .rev()
            .fold(None, |child, node| {
                Some(VersionedEntry::new(
                    node.entry.clone(),
                    node.timestamp(),
                    node.is_deleted(),
                    child,
                ))
            })
            .expect("retained chain must keep at least one node");
        rebuilt_heads.push((key, new_head));
    }

    for (key, head) in rebuilt_heads {
        map.update_entry(&key, head);
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{CollectionFamily, InstallMode};
    use crate::entry::{CatalogEntryEnum, SchemaEntry};
    use crate::mvcc::CatalogSnapshot;
    use paro_storage::transaction::manager::TRANSACTION_ID_START;

    fn test_set() -> Arc<CatalogCollection> {
        CatalogCollection::new_for_tests("test", 1, CollectionFamily::Schemas)
    }

    fn schema_entry(name: &str) -> Arc<CatalogEntryEnum> {
        Arc::new(CatalogEntryEnum::Schema(Arc::new(SchemaEntry::new(
            "test_catalog".to_string(),
            name.to_string(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            0,
        ))))
    }

    fn published_replace(
        set: &Arc<CatalogCollection>,
        name: &str,
        writer_id: u64,
        start_time: u64,
        commit_id: u64,
    ) {
        let snapshot = CatalogSnapshot::writer(writer_id, start_time);
        let handle = set
            .stage_replace(&snapshot, name, schema_entry(name))
            .expect("stage replace")
            .expect("replace handle");
        handle.publish(commit_id).expect("publish replace");
    }

    fn published_drop(
        set: &Arc<CatalogCollection>,
        name: &str,
        writer_id: u64,
        start_time: u64,
        commit_id: u64,
    ) {
        let snapshot = CatalogSnapshot::writer(writer_id, start_time);
        let handle = set
            .stage_drop(&snapshot, name)
            .expect("stage drop")
            .expect("drop handle");
        handle.publish(commit_id).expect("publish drop");
    }

    fn chain_timestamps(set: &CatalogCollection, key: &str) -> Vec<u64> {
        let mut timestamps = Vec::new();
        let lower = key.to_lowercase();
        set.for_each_chain_head(|name, head| {
            if name != lower {
                return;
            }
            let mut current = Some(Arc::clone(head));
            while let Some(node) = current {
                timestamps.push(node.timestamp());
                current = node.child();
            }
        });
        timestamps
    }

    fn chain_deleted(set: &CatalogCollection, key: &str) -> Vec<bool> {
        let mut deleted = Vec::new();
        let lower = key.to_lowercase();
        set.for_each_chain_head(|name, head| {
            if name != lower {
                return;
            }
            let mut current = Some(Arc::clone(head));
            while let Some(node) = current {
                deleted.push(node.is_deleted());
                current = node.child();
            }
        });
        deleted
    }

    fn chain_head(set: &CatalogCollection, key: &str) -> Arc<VersionedEntry> {
        let mut found = None;
        let lower = key.to_lowercase();
        set.for_each_chain_head(|name, head| {
            if name == lower {
                found = Some(Arc::clone(head));
            }
        });
        found.expect("head should exist")
    }

    #[test]
    fn gc_skips_chain_when_head_is_provisional() {
        let set = test_set();
        set.install_committed(schema_entry("item"), InstallMode::RejectExisting)
            .expect("install committed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 5, 10);

        let snapshot = CatalogSnapshot::writer(TRANSACTION_ID_START + 2, 11);
        let _staged = set
            .stage_replace(&snapshot, "item", schema_entry("item"))
            .expect("stage replace")
            .expect("replace handle");
        let before = chain_head(&set, "item");

        let stats = run_collection_gc(set.as_ref(), 100);

        let after = chain_head(&set, "item");
        assert_eq!(stats.chains_scanned, 1);
        assert_eq!(stats.chains_skipped_provisional, 1);
        assert_eq!(stats.chains_rebuilt, 0);
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn gc_keeps_permanent_node_outside_committed_anchor_budget() {
        let set = test_set();
        set.install_committed(schema_entry("item"), InstallMode::RejectExisting)
            .expect("install committed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 5, 10);
        published_replace(&set, "item", TRANSACTION_ID_START + 2, 11, 20);

        let stats = run_collection_gc(set.as_ref(), 21);

        assert_eq!(stats.chains_rebuilt, 1);
        assert_eq!(stats.nodes_pruned, 1);
        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![20, 0]);
    }

    #[test]
    fn gc_treats_tombstone_as_regular_committed_anchor() {
        let set = test_set();
        set.install_committed(schema_entry("item"), InstallMode::RejectExisting)
            .expect("install committed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 5, 10);
        published_drop(&set, "item", TRANSACTION_ID_START + 2, 11, 20);

        let stats = run_collection_gc(set.as_ref(), 21);

        assert_eq!(stats.chains_rebuilt, 1);
        assert_eq!(stats.nodes_pruned, 1);
        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![20, 0]);
        assert_eq!(chain_deleted(set.as_ref(), "item"), vec![true, false]);
    }

    #[test]
    fn gc_prunes_each_key_independently() {
        let set = test_set();
        set.install_committed(schema_entry("item_a"), InstallMode::RejectExisting)
            .expect("install item_a");
        set.install_committed(schema_entry("item_b"), InstallMode::RejectExisting)
            .expect("install item_b");
        published_replace(&set, "item_a", TRANSACTION_ID_START + 1, 5, 10);
        published_replace(&set, "item_a", TRANSACTION_ID_START + 2, 11, 20);

        let before_b = chain_head(set.as_ref(), "item_b");
        let stats = run_collection_gc(set.as_ref(), 21);
        let after_b = chain_head(set.as_ref(), "item_b");

        assert_eq!(stats.chains_scanned, 2);
        assert_eq!(stats.chains_rebuilt, 1);
        assert_eq!(chain_timestamps(set.as_ref(), "item_a"), vec![20, 0]);
        assert!(Arc::ptr_eq(&before_b, &after_b));
        assert_eq!(chain_timestamps(set.as_ref(), "item_b"), vec![0]);
    }

    #[test]
    fn gc_skips_empty_collection() {
        let set = test_set();

        let stats = run_collection_gc(set.as_ref(), 100);

        assert_eq!(stats.chains_scanned, 0);
        assert_eq!(stats.chains_rebuilt, 0);
        assert_eq!(stats.nodes_pruned, 0);
    }

    #[test]
    fn gc_skips_trivial_single_node_chain() {
        let set = test_set();
        set.install_committed(schema_entry("item"), InstallMode::RejectExisting)
            .expect("install committed");

        let before = chain_head(set.as_ref(), "item");
        let stats = run_collection_gc(set.as_ref(), 100);
        let after = chain_head(set.as_ref(), "item");

        assert_eq!(stats.chains_scanned, 1);
        assert_eq!(stats.chains_skipped_trivial, 1);
        assert_eq!(stats.chains_rebuilt, 0);
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn gc_keeps_only_latest_committed_anchor_when_chain_has_no_permanent_node() {
        let set = test_set();
        set.install_replayed(5, schema_entry("item"), InstallMode::RejectExisting)
            .expect("install replayed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 6, 10);
        published_replace(&set, "item", TRANSACTION_ID_START + 2, 11, 20);

        let stats = run_collection_gc(set.as_ref(), 21);

        assert_eq!(stats.chains_rebuilt, 1);
        assert_eq!(stats.nodes_pruned, 2);
        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![20]);
    }

    #[test]
    fn install_replayed_replaces_existing_chain_instead_of_appending() {
        let set = test_set();
        set.install_replayed(5, schema_entry("item"), InstallMode::RejectExisting)
            .expect("install first replayed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 6, 10);

        set.install_replayed(15, schema_entry("item"), InstallMode::ReplaceExisting)
            .expect("replace replayed");

        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![15]);
    }

    #[test]
    fn publish_after_provisional_skip_does_not_leave_zombie_head() {
        let set = test_set();
        set.install_committed(schema_entry("item"), InstallMode::RejectExisting)
            .expect("install committed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 5, 10);

        let snapshot = CatalogSnapshot::writer(TRANSACTION_ID_START + 2, 11);
        let handle = set
            .stage_replace(&snapshot, "item", schema_entry("item"))
            .expect("stage replace")
            .expect("replace handle");

        let skipped = run_collection_gc(set.as_ref(), 100);
        assert_eq!(skipped.chains_skipped_provisional, 1);

        handle.publish(20).expect("publish replace");

        let pruned = run_collection_gc(set.as_ref(), 21);
        assert_eq!(pruned.chains_rebuilt, 1);
        assert_eq!(pruned.nodes_pruned, 1);
        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![20, 0]);
        assert_eq!(chain_deleted(set.as_ref(), "item"), vec![false, false]);
    }

    #[test]
    fn discard_after_provisional_skip_allows_next_gc_to_prune_old_tail() {
        let set = test_set();
        set.install_committed(schema_entry("item"), InstallMode::RejectExisting)
            .expect("install committed");
        published_replace(&set, "item", TRANSACTION_ID_START + 1, 5, 10);
        published_replace(&set, "item", TRANSACTION_ID_START + 2, 11, 20);

        let snapshot = CatalogSnapshot::writer(TRANSACTION_ID_START + 3, 21);
        let handle = set
            .stage_replace(&snapshot, "item", schema_entry("item"))
            .expect("stage replace")
            .expect("replace handle");

        let skipped = run_collection_gc(set.as_ref(), 100);
        assert_eq!(skipped.chains_skipped_provisional, 1);

        handle.discard().expect("discard replace");
        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![20, 10, 0]);

        let pruned = run_collection_gc(set.as_ref(), 21);
        assert_eq!(pruned.chains_rebuilt, 1);
        assert_eq!(pruned.nodes_pruned, 1);
        assert_eq!(chain_timestamps(set.as_ref(), "item"), vec![20, 0]);
    }
}
