// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{PhysicalRowRef, Tablet};
use paro_common::durability::PrepareToken;
use paro_common::effect::{
    encode_delete_patch_artifact_bytes, ArtifactRef, DeletePatchEncoding, DeletePatchGroup,
    DeletePatchInline, DeletePatchRef, DeletePatchSegment, VersionSpan,
};
use paro_common::error::{self as paro_error, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const DEFAULT_DELETE_PATCH_INLINE_ROW_REF_THRESHOLD: usize = 256;
static DELETE_PATCH_INLINE_ROW_REF_THRESHOLD: AtomicUsize =
    AtomicUsize::new(DEFAULT_DELETE_PATCH_INLINE_ROW_REF_THRESHOLD);
#[cfg(test)]
static PREPARE_SNAPSHOT_FORCED_RETRIES: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PREPARE_SNAPSHOT_SLOW_PATH_HITS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VisibleRowsetRef {
    pub rowset_id: u64,
    pub version_span: VersionSpan,
    pub segment_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrimaryIndexReadView {
    locations: HashMap<Vec<u8>, PhysicalRowRef>,
}

impl PrimaryIndexReadView {
    fn capture(tablet: &Tablet, keys: &[Vec<u8>]) -> Result<Option<Self>> {
        if keys.is_empty() {
            return Ok(None);
        }

        let resolved = tablet.lookup_primary_keys(keys)?;
        let mut locations = HashMap::new();
        for (key, row_id) in keys.iter().zip(resolved.into_iter()) {
            let Some(row_id) = row_id else {
                continue;
            };
            locations.insert(key.clone(), tablet.decode_row_id(row_id)?);
        }
        Ok(Some(Self { locations }))
    }

    pub fn lookup(&self, key: &[u8]) -> Option<PhysicalRowRef> {
        self.locations.get(key).copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrepareSnapshot {
    pub tablet_id: u64,
    pub visible_version: i64,
    pub rowset_epoch: u64,
    pub schema_epoch: Option<u64>,
    pub visible_rowsets: Arc<[VisibleRowsetRef]>,
    pub primary_index_view: Option<PrimaryIndexReadView>,
}

impl PrepareSnapshot {
    pub fn prepare_token(&self) -> PrepareToken {
        PrepareToken {
            visible_version: self.visible_version,
            rowset_epoch: self.rowset_epoch,
            schema_epoch: self.schema_epoch,
        }
    }

    fn visible_rowset(&self, rowset_id: u64) -> Option<&VisibleRowsetRef> {
        self.visible_rowsets
            .iter()
            .find(|rowset| rowset.rowset_id == rowset_id)
    }

    fn validate_row_ref(&self, location: PhysicalRowRef) -> Result<()> {
        let rowset = self.visible_rowset(location.rowset_id).ok_or_else(|| {
            paro_error::serialization_failure(format!(
                "tablet {} prepare snapshot no longer exposes rowset {}",
                self.tablet_id, location.rowset_id
            ))
        })?;
        if location.segment_id >= rowset.segment_count {
            return Err(paro_error::serialization_failure(format!(
                "tablet {} prepare snapshot rowset {} no longer exposes segment {}",
                self.tablet_id, location.rowset_id, location.segment_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedDeletePatch {
    pub patch_ref: DeletePatchRef,
    pub deleted_row_count: u32,
    pub artifact_path: Option<PathBuf>,
}

pub(crate) fn delete_patch_inline_row_ref_threshold() -> usize {
    DELETE_PATCH_INLINE_ROW_REF_THRESHOLD
        .load(Ordering::Relaxed)
        .max(1)
}

pub(crate) fn set_delete_patch_inline_row_ref_threshold(threshold: usize) {
    DELETE_PATCH_INLINE_ROW_REF_THRESHOLD.store(threshold.max(1), Ordering::Relaxed);
}

pub(crate) fn current_delete_patch_inline_row_ref_threshold() -> usize {
    delete_patch_inline_row_ref_threshold()
}

#[cfg(test)]
pub(crate) fn force_prepare_snapshot_retries(retries: usize) {
    PREPARE_SNAPSHOT_FORCED_RETRIES.store(retries, Ordering::Release);
}

#[cfg(test)]
pub(crate) fn take_prepare_snapshot_slow_path_hits() -> usize {
    PREPARE_SNAPSHOT_SLOW_PATH_HITS.swap(0, Ordering::AcqRel)
}

pub(crate) fn capture_prepare_snapshot(
    tablet: &Tablet,
    lookup_keys: &[Vec<u8>],
) -> Result<PrepareSnapshot> {
    for _attempt in 0..3 {
        let visible_version = tablet.max_version();
        let rowset_epoch = tablet.rowset_epoch();
        let schema_epoch = tablet.schema_epoch();
        let visible_rowsets = tablet
            .capture_consistent_rowsets(visible_version)?
            .into_iter()
            .map(|rowset| VisibleRowsetRef {
                rowset_id: rowset.rowset_id(),
                version_span: VersionSpan {
                    start: rowset.start_version(),
                    end: rowset.end_version(),
                },
                segment_count: rowset.segments().len() as u32,
            })
            .collect::<Vec<_>>();
        let primary_index_view = PrimaryIndexReadView::capture(tablet, lookup_keys)?;

        #[cfg(test)]
        if PREPARE_SNAPSHOT_FORCED_RETRIES
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining > 0).then(|| remaining - 1)
            })
            .is_ok()
        {
            continue;
        }

        if tablet.max_version() == visible_version
            && tablet.rowset_epoch() == rowset_epoch
            && tablet.schema_epoch() == schema_epoch
        {
            return Ok(PrepareSnapshot {
                tablet_id: tablet.tablet_id(),
                visible_version,
                rowset_epoch,
                schema_epoch,
                visible_rowsets: Arc::from(visible_rowsets),
                primary_index_view,
            });
        }
    }

    tablet.with_prepare_snapshot_lock(|| {
        #[cfg(test)]
        PREPARE_SNAPSHOT_SLOW_PATH_HITS.fetch_add(1, Ordering::AcqRel);
        let visible_version = tablet.max_version();
        let rowset_epoch = tablet.rowset_epoch();
        let schema_epoch = tablet.schema_epoch();
        let visible_rowsets = tablet
            .capture_consistent_rowsets(visible_version)?
            .into_iter()
            .map(|rowset| VisibleRowsetRef {
                rowset_id: rowset.rowset_id(),
                version_span: VersionSpan {
                    start: rowset.start_version(),
                    end: rowset.end_version(),
                },
                segment_count: rowset.segments().len() as u32,
            })
            .collect::<Vec<_>>();
        let primary_index_view = PrimaryIndexReadView::capture(tablet, lookup_keys)?;
        Ok(PrepareSnapshot {
            tablet_id: tablet.tablet_id(),
            visible_version,
            rowset_epoch,
            schema_epoch,
            visible_rowsets: Arc::from(visible_rowsets),
            primary_index_view,
        })
    })
}

pub(crate) fn build_delete_patch_from_primary_keys(
    snapshot: &PrepareSnapshot,
    keys: &[Vec<u8>],
) -> Result<Option<DeletePatchInline>> {
    let Some(index_view) = snapshot.primary_index_view.as_ref() else {
        return Ok(None);
    };

    let mut locations = Vec::new();
    for key in keys {
        let Some(location) = index_view.lookup(key) else {
            continue;
        };
        snapshot.validate_row_ref(location)?;
        locations.push(location);
    }
    build_delete_patch_from_row_refs(snapshot, &locations)
}

pub(crate) fn build_delete_patch_from_row_refs(
    snapshot: &PrepareSnapshot,
    row_refs: &[PhysicalRowRef],
) -> Result<Option<DeletePatchInline>> {
    let mut deduped = BTreeSet::new();
    for location in row_refs.iter().copied() {
        snapshot.validate_row_ref(location)?;
        deduped.insert((location.rowset_id, location.segment_id, location.row_offset));
    }

    if deduped.is_empty() {
        return Ok(None);
    }

    let mut grouped: BTreeMap<u64, BTreeMap<u32, Vec<u32>>> = BTreeMap::new();
    for (rowset_id, segment_id, row_offset) in deduped {
        grouped
            .entry(rowset_id)
            .or_default()
            .entry(segment_id)
            .or_default()
            .push(row_offset);
    }

    let row_count = grouped
        .values()
        .map(|segments| segments.values().map(Vec::len).sum::<usize>())
        .sum::<usize>() as u32;
    let groups = grouped
        .into_iter()
        .map(|(rowset_id, segments)| DeletePatchGroup {
            rowset_id,
            segments: segments
                .into_iter()
                .map(|(segment_id, offsets)| DeletePatchSegment {
                    segment_id,
                    row_offsets_delta: encode_row_offsets_delta(&offsets),
                })
                .collect(),
        })
        .collect();

    Ok(Some(DeletePatchInline {
        encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
        row_count,
        groups,
    }))
}

fn encode_row_offsets_delta(offsets: &[u32]) -> Vec<u32> {
    let mut encoded = Vec::with_capacity(offsets.len());
    let mut previous = 0u32;
    for (index, row_offset) in offsets.iter().copied().enumerate() {
        if index == 0 {
            encoded.push(row_offset);
        } else {
            encoded.push(row_offset - previous);
        }
        previous = row_offset;
    }
    encoded
}

pub(crate) fn materialize_delete_patch(
    tablet: &Tablet,
    txn_id: u64,
    ordinal: usize,
    patch: DeletePatchInline,
) -> Result<MaterializedDeletePatch> {
    let deleted_row_count = patch.row_count;
    if (deleted_row_count as usize) <= delete_patch_inline_row_ref_threshold() {
        return Ok(MaterializedDeletePatch {
            patch_ref: DeletePatchRef::Inline(patch),
            deleted_row_count,
            artifact_path: None,
        });
    }

    let artifact_path = delete_patch_artifact_path(tablet, txn_id, ordinal);
    if let Some(parent) = artifact_path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            paro_error::io_error(format!(
                "create delete patch artifact parent {}: {}",
                parent.display(),
                err
            ))
        })?;
    }
    std::fs::write(&artifact_path, encode_delete_patch_artifact_bytes(&patch)?).map_err(|err| {
        paro_error::io_error(format!(
            "write delete patch artifact {}: {}",
            artifact_path.display(),
            err
        ))
    })?;
    Tablet::sync_parent_dir(&artifact_path)?;

    Ok(MaterializedDeletePatch {
        patch_ref: DeletePatchRef::Artifact(ArtifactRef::from_tablet_path(
            tablet.data_dir(),
            &artifact_path,
        )?),
        deleted_row_count,
        artifact_path: Some(artifact_path),
    })
}

fn delete_patch_artifact_path(tablet: &Tablet, txn_id: u64, ordinal: usize) -> PathBuf {
    tablet
        .data_dir()
        .join("_delete_patch")
        .join(format!("txn_{txn_id}"))
        .join(format!("patch_{ordinal}.bin"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use tempfile::TempDir;

    fn snapshot() -> PrepareSnapshot {
        PrepareSnapshot {
            tablet_id: 9,
            visible_version: 11,
            rowset_epoch: 3,
            schema_epoch: Some(5),
            visible_rowsets: Arc::from([
                VisibleRowsetRef {
                    rowset_id: 10,
                    version_span: VersionSpan { start: 7, end: 7 },
                    segment_count: 2,
                },
                VisibleRowsetRef {
                    rowset_id: 11,
                    version_span: VersionSpan { start: 8, end: 8 },
                    segment_count: 1,
                },
            ]),
            primary_index_view: Some(PrimaryIndexReadView {
                locations: HashMap::from([
                    (b"a".to_vec(), PhysicalRowRef::new(10, 0, 3)),
                    (b"b".to_vec(), PhysicalRowRef::new(10, 0, 7)),
                ]),
            }),
        }
    }

    #[test]
    fn delete_patch_builder_dedups_and_delta_encodes() {
        let patch = build_delete_patch_from_row_refs(
            &snapshot(),
            &[
                PhysicalRowRef::new(10, 0, 3),
                PhysicalRowRef::new(10, 0, 7),
                PhysicalRowRef::new(10, 0, 7),
                PhysicalRowRef::new(11, 0, 2),
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(patch.row_count, 3);
        assert_eq!(patch.groups[0].segments[0].row_offsets_delta, vec![3, 4]);
        assert_eq!(patch.groups[1].segments[0].row_offsets_delta, vec![2]);
    }

    #[test]
    fn empty_primary_delete_patch_is_omitted() {
        let patch =
            build_delete_patch_from_primary_keys(&snapshot(), &[b"missing".to_vec()]).unwrap();
        assert!(patch.is_none());
    }

    #[test]
    fn stale_row_ref_is_rejected() {
        let err = build_delete_patch_from_row_refs(&snapshot(), &[PhysicalRowRef::new(999, 0, 1)])
            .unwrap_err();
        assert!(err.to_string().contains("no longer exposes rowset"));
    }

    #[test]
    fn large_delete_patch_materializes_to_artifact() {
        let tmp = TempDir::new().unwrap();
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::key(0, "id", LogicalType::Integer)],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let tablet = Tablet::new(99, 99, 0, schema, tmp.path(), None).unwrap();

        let previous = delete_patch_inline_row_ref_threshold();
        set_delete_patch_inline_row_ref_threshold(2);
        let materialized = materialize_delete_patch(
            &tablet,
            42,
            0,
            DeletePatchInline {
                encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                row_count: 3,
                groups: vec![DeletePatchGroup {
                    rowset_id: 10,
                    segments: vec![DeletePatchSegment {
                        segment_id: 0,
                        row_offsets_delta: vec![3, 4, 5],
                    }],
                }],
            },
        )
        .unwrap();
        set_delete_patch_inline_row_ref_threshold(previous);

        let DeletePatchRef::Artifact(reference) = materialized.patch_ref else {
            panic!("expected artifact-backed patch");
        };
        assert_eq!(materialized.deleted_row_count, 3);
        assert!(materialized.artifact_path.as_ref().unwrap().exists());
        assert_eq!(
            DeletePatchRef::Artifact(reference)
                .decode_row_refs_for_tablet(tablet.data_dir())
                .unwrap(),
            vec![(10, 0, 3), (10, 0, 7), (10, 0, 12)]
        );
    }

    #[test]
    fn capture_prepare_snapshot_falls_back_to_locked_slow_path_after_repeated_instability() {
        let tmp = TempDir::new().unwrap();
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::key(0, "id", LogicalType::Integer)],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let tablet = Tablet::new(99, 99, 0, schema, tmp.path(), None).unwrap();
        tablet.init().unwrap();

        force_prepare_snapshot_retries(3);
        assert_eq!(take_prepare_snapshot_slow_path_hits(), 0);

        let snapshot = capture_prepare_snapshot(&tablet, &[]).unwrap();

        assert_eq!(snapshot.tablet_id, 99);
        assert_eq!(take_prepare_snapshot_slow_path_hits(), 1);
        force_prepare_snapshot_retries(0);
    }
}
