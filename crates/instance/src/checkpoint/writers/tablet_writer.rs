// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::checkpoint::view::CheckpointView;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::{
    CheckpointDeleteVectorBundle, CheckpointRowsetBundle, CheckpointTabletBundle,
    CheckpointTabletIdentity, DurableTabletFreezeMode, TabletShardBundle,
};
use paro_storage::primary_key::DeleteVector;
use paro_storage::rowset::RowsetSharedPtr;
use paro_storage::tablet::{CheckpointTabletFreezeMode, CheckpointTabletSnapshot};

const CHECKPOINT_CAPTURE_OPTIMISTIC_RETRIES: usize = 4;
const TARGET_ROWSETS_PER_SHARD: usize = 128;

#[derive(Debug, Default)]
pub struct TabletWriter;

#[derive(Debug)]
struct TabletCandidate {
    weight: usize,
    bundle: CheckpointTabletBundle,
}

impl TabletWriter {
    pub fn serialize_view(
        catalog: &ParoCatalog,
        view: &CheckpointView,
    ) -> anyhow::Result<Vec<TabletShardBundle>> {
        let txn = CatalogSnapshot::read_only(view.catalog_snapshot_ts);
        let mut candidates = Vec::new();

        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            let Some(tables) = schema.collection(CatalogType::Table) else {
                continue;
            };
            for table_entry in tables.scan(txn.transaction_id, txn.start_time) {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };
                let storage = table.get_storage().ok_or_else(|| {
                    anyhow::anyhow!(
                        "table {}.{} is visible at checkpoint but has no live tablet handle",
                        schema.base.name,
                        table.name()
                    )
                })?;
                let snapshot = storage
                    .tablet()
                    .capture_checkpoint_snapshot(
                        view.frontier.checkpoint_commit_id,
                        view.frontier.checkpoint_maintenance_id,
                        CHECKPOINT_CAPTURE_OPTIMISTIC_RETRIES,
                    )
                    .map_err(anyhow::Error::from)?;
                let bundle = Self::snapshot_bundle(storage.tablet().as_ref(), snapshot)?;
                candidates.push(TabletCandidate {
                    weight: bundle.visible_rowset_count.max(1) as usize,
                    bundle,
                });
            }
        }

        Ok(Self::pack_shards(candidates))
    }

    fn snapshot_bundle(
        tablet: &paro_storage::tablet::Tablet,
        snapshot: CheckpointTabletSnapshot,
    ) -> anyhow::Result<CheckpointTabletBundle> {
        let mut rowsets = Vec::with_capacity(snapshot.rowsets.len());
        for rowset in &snapshot.rowsets {
            rowsets.push(Self::rowset_bundle(rowset, snapshot.visible_version)?);
        }

        let meta_bytes = tablet
            .capture_checkpoint_meta_bytes(&snapshot)
            .map_err(anyhow::Error::from)?;

        Ok(CheckpointTabletBundle {
            identity: CheckpointTabletIdentity {
                table_id: snapshot.identity.table_id,
                partition_id: snapshot.identity.partition_id,
                tablet_id: snapshot.identity.tablet_id,
                schema_id: snapshot.identity.schema_id,
                schema_version: snapshot.identity.schema_version,
            },
            visible_rowset_count: snapshot.rowsets.len() as u32,
            visible_version: snapshot.visible_version,
            max_version: snapshot.max_version,
            cumulative_point: snapshot.cumulative_point,
            freeze_mode: match snapshot.freeze_mode {
                CheckpointTabletFreezeMode::Optimistic => DurableTabletFreezeMode::Optimistic,
                CheckpointTabletFreezeMode::MetaLock => DurableTabletFreezeMode::MetaLock,
            },
            meta_bytes,
            rowsets,
        })
    }

    fn rowset_bundle(
        rowset: &RowsetSharedPtr,
        visible_version: i64,
    ) -> anyhow::Result<CheckpointRowsetBundle> {
        let rowset_meta = rowset.rowset_meta();
        let mut delete_vectors = Vec::new();
        for segment_id in 0..rowset.num_segments() {
            let delete_vector = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                segment_id,
                visible_version,
            )
            .map_err(anyhow::Error::from)?;
            let Some(delete_vector) = delete_vector else {
                continue;
            };
            delete_vectors.push(CheckpointDeleteVectorBundle {
                segment_id,
                version: delete_vector.version(),
                payload: delete_vector.to_bytes().map_err(anyhow::Error::from)?,
            });
        }

        Ok(CheckpointRowsetBundle {
            rowset_id: rowset_meta.rowset_id(),
            meta_bytes: rowset_meta.serialize().map_err(anyhow::Error::from)?,
            delete_vectors,
        })
    }

    fn pack_shards(mut candidates: Vec<TabletCandidate>) -> Vec<TabletShardBundle> {
        if candidates.is_empty() {
            return Vec::new();
        }

        let total_weight: usize = candidates.iter().map(|candidate| candidate.weight).sum();
        let shard_count = total_weight
            .div_ceil(TARGET_ROWSETS_PER_SHARD)
            .clamp(1, candidates.len());

        candidates.sort_by(|left, right| {
            right.weight.cmp(&left.weight).then(
                left.bundle
                    .identity
                    .tablet_id
                    .cmp(&right.bundle.identity.tablet_id),
            )
        });

        let mut loads = vec![0usize; shard_count];
        let mut shards: Vec<Vec<CheckpointTabletBundle>> =
            (0..shard_count).map(|_| Vec::new()).collect();

        for candidate in candidates {
            let shard_idx = loads
                .iter()
                .enumerate()
                .min_by_key(|(idx, load)| (**load, *idx))
                .map(|(idx, _)| idx)
                .expect("at least one shard exists");
            loads[shard_idx] += candidate.weight;
            shards[shard_idx].push(candidate.bundle);
        }

        let mut bundles = Vec::new();
        for (shard_id, mut tablets) in shards.into_iter().enumerate() {
            if tablets.is_empty() {
                continue;
            }
            tablets.sort_by_key(|tablet| tablet.identity.tablet_id);
            bundles.push(TabletShardBundle {
                shard_id: shard_id as u32,
                tablets,
            });
        }
        bundles
    }
}

#[cfg(test)]
mod tests {
    use super::TabletWriter;
    use crate::checkpoint::view::{CheckpointCut, CheckpointView};
    use paro_catalog::catalog::Catalog;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::database_catalog::ParoCatalog;
    use paro_catalog::entry::{
        CatalogEntryEnum, CatalogType, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::checkpoint::{CheckpointFrontier, RecoverySummary};
    use paro_common::types::LogicalType;
    use paro_storage::table::table_factory::TableFactory;
    use std::sync::Arc;

    fn aligned_view() -> CheckpointView {
        CheckpointView::new(
            CheckpointCut {
                target_lsn: 4,
                issued_at_micros: 1,
            },
            CheckpointFrontier {
                checkpoint_lsn: 4,
                checkpoint_commit_id: 10,
                checkpoint_maintenance_id: 0,
            },
            RecoverySummary {
                max_lsn: 4,
                max_commit_id: 10,
                max_maintenance_id: 0,
                max_catalog_commit_id: 10,
                max_seen_object_id: 200,
            },
            11,
        )
        .expect("checkpoint view should be aligned")
    }

    #[test]
    fn serialize_view_excludes_post_cut_tablets_from_snapshot() {
        let catalog = ParoCatalog::new("test_db".to_string());
        catalog.initialize(false);
        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog
            .get_schema(&read_txn, "public")
            .expect("public schema");
        let tables = schema
            .collection(CatalogType::Table)
            .expect("table collection");

        let before_cut_storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("create before-cut table"),
        );
        tables
            .install_replayed(
                10,
                Arc::new(CatalogEntryEnum::Table(Arc::new(
                    TableCatalogEntry::from_info(
                        CreateTableInfo::new(
                            catalog.name().to_string(),
                            "public".to_string(),
                            "before_cut".to_string(),
                            vec![ColumnDefinition::new(
                                "id".to_string(),
                                LogicalType::Integer,
                            )],
                        ),
                        before_cut_storage.clone(),
                        10,
                    ),
                ))),
                InstallMode::RejectExisting,
            )
            .expect("install before-cut table");

        let after_cut_storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("create after-cut table"),
        );
        tables
            .install_replayed(
                11,
                Arc::new(CatalogEntryEnum::Table(Arc::new(
                    TableCatalogEntry::from_info(
                        CreateTableInfo::new(
                            catalog.name().to_string(),
                            "public".to_string(),
                            "after_cut".to_string(),
                            vec![ColumnDefinition::new(
                                "id".to_string(),
                                LogicalType::Integer,
                            )],
                        ),
                        after_cut_storage,
                        11,
                    ),
                ))),
                InstallMode::RejectExisting,
            )
            .expect("install after-cut table");

        let shards = TabletWriter::serialize_view(&catalog, &aligned_view())
            .expect("serialize tablet snapshot");
        let tablet_ids: Vec<u64> = shards
            .iter()
            .flat_map(|shard| shard.tablets.iter().map(|tablet| tablet.identity.tablet_id))
            .collect();

        assert_eq!(tablet_ids, vec![before_cut_storage.tablet_id()]);
    }
}
