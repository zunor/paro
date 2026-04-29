// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::error as paro_error;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

const DELETE_PATCH_ARTIFACT_MAGIC: [u8; 4] = *b"DPCH";
const DELETE_PATCH_ARTIFACT_VERSION: u32 = 1;
const LEGACY_COMPACTION_STAGING_ROOT: &str = "_compaction";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactNamespace {
    CanonicalRowset,
    Staged,
    DeletePatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub namespace: ArtifactNamespace,
    pub locator: Vec<String>,
}

impl ArtifactRef {
    pub fn from_tablet_path(tablet_data_dir: &Path, path: &Path) -> Result<Self> {
        let namespaces = [
            (
                ArtifactNamespace::CanonicalRowset,
                tablet_data_dir.join("rowsets"),
            ),
            (ArtifactNamespace::Staged, tablet_data_dir.join("_staged")),
            (
                ArtifactNamespace::DeletePatch,
                tablet_data_dir.join("_delete_patch"),
            ),
        ];
        for (namespace, root) in namespaces {
            if let Ok(relative) = path.strip_prefix(&root) {
                return Ok(Self {
                    namespace,
                    locator: path_components(relative),
                });
            }
        }
        if let Ok(relative) =
            path.strip_prefix(tablet_data_dir.join(LEGACY_COMPACTION_STAGING_ROOT))
        {
            let mut locator = Vec::with_capacity(1 + relative.components().count());
            locator.push(LEGACY_COMPACTION_STAGING_ROOT.to_string());
            locator.extend(path_components(relative));
            return Ok(Self {
                namespace: ArtifactNamespace::Staged,
                locator,
            });
        }
        Err(paro_error::invalid_input(format!(
            "artifact path {} is not under tablet data dir {}",
            path.display(),
            tablet_data_dir.display()
        )))
    }

    pub fn resolve_for_tablet(&self, tablet_data_dir: &Path) -> PathBuf {
        match self.namespace {
            ArtifactNamespace::CanonicalRowset => {
                let mut path = tablet_data_dir.join("rowsets");
                for component in &self.locator {
                    path.push(component);
                }
                path
            }
            ArtifactNamespace::Staged => {
                let is_legacy_compaction = self
                    .locator
                    .first()
                    .is_some_and(|component| component == LEGACY_COMPACTION_STAGING_ROOT);
                let mut path = if is_legacy_compaction {
                    tablet_data_dir.join(LEGACY_COMPACTION_STAGING_ROOT)
                } else {
                    tablet_data_dir.join("_staged")
                };
                for component in self.locator.iter().skip(usize::from(is_legacy_compaction)) {
                    path.push(component);
                }
                path
            }
            ArtifactNamespace::DeletePatch => {
                let mut path = tablet_data_dir.join("_delete_patch");
                for component in &self.locator {
                    path.push(component);
                }
                path
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowsetLocator {
    pub tablet_id: u64,
    pub rowset_id: u64,
    pub path_components: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreparedDataOp {
    RowsetCommit {
        locator: RowsetLocator,
        start_version: i64,
        end_version: i64,
    },
    PrimaryDelete {
        tablet_id: u64,
        keys: Vec<Vec<u8>>,
    },
    RowIdDelete {
        tablet_id: u64,
        locations: Vec<(u64, u32, u32)>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionSpan {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionCumulativePointAction {
    Preserve,
    AdvanceToOutputEndExclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredRowsetInput {
    pub rowset_id: u64,
    pub start_version: i64,
    pub end_version: i64,
    pub rssids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageCommitOp {
    Tablet(TabletApplyOp),
}

impl StorageCommitOp {
    pub fn tablet_id(&self) -> u64 {
        match self {
            Self::Tablet(op) => op.tablet_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletApplyOp {
    pub tablet_id: u64,
    pub mutations: Vec<TabletMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TabletMutation {
    PublishRowset {
        rowset_id: u64,
        version_span: VersionSpan,
        rowset_ref: ArtifactRef,
    },
    ApplyPrimaryDelete {
        keys: Vec<Vec<u8>>,
    },
    ApplyDeletePatch {
        patch: DeletePatchRef,
        deleted_row_count: u32,
    },
    PublishCompaction {
        plan_id: u64,
        job_id: u64,
        output_rowset_id: u64,
        output_version: VersionSpan,
        staged_ref: ArtifactRef,
        output_ref: ArtifactRef,
        replaced_inputs: Vec<u64>,
        retired_inputs: Vec<RetiredRowsetInput>,
        cumulative_point_action: CompactionCumulativePointAction,
    },
}

impl TabletMutation {
    pub fn tablet_rowset_id(&self) -> Option<u64> {
        match self {
            Self::PublishRowset { rowset_id, .. } => Some(*rowset_id),
            Self::PublishCompaction {
                output_rowset_id, ..
            } => Some(*output_rowset_id),
            Self::ApplyPrimaryDelete { .. } | Self::ApplyDeletePatch { .. } => None,
        }
    }

    pub fn stable_artifact_id(&self) -> u64 {
        match self {
            Self::PublishRowset { rowset_id, .. } => *rowset_id,
            Self::PublishCompaction {
                output_rowset_id, ..
            } => *output_rowset_id,
            Self::ApplyPrimaryDelete { keys } => stable_hash_keys(keys),
            Self::ApplyDeletePatch { patch, .. } => stable_hash_delete_patch(patch),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeletePatchEncoding {
    GroupedRowOffsetDeltaV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeletePatchRef {
    Inline(DeletePatchInline),
    Artifact(ArtifactRef),
}

impl DeletePatchRef {
    pub fn row_count(&self) -> u32 {
        match self {
            Self::Inline(patch) => patch.row_count,
            Self::Artifact(_) => 0,
        }
    }

    pub fn decode_row_refs_for_tablet(
        &self,
        tablet_data_dir: &Path,
    ) -> Result<Vec<(u64, u32, u32)>> {
        match self {
            Self::Inline(patch) => patch.decode_row_refs(),
            Self::Artifact(reference) => {
                let artifact_path = reference.resolve_for_tablet(tablet_data_dir);
                let bytes = std::fs::read(&artifact_path).map_err(|err| {
                    paro_error::io_error(format!(
                        "read delete patch artifact {}: {}",
                        artifact_path.display(),
                        err
                    ))
                })?;
                decode_delete_patch_artifact_bytes(&bytes)?.decode_row_refs()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletePatchInline {
    pub encoding: DeletePatchEncoding,
    pub row_count: u32,
    pub groups: Vec<DeletePatchGroup>,
}

fn stable_hash_keys(keys: &[Vec<u8>]) -> u64 {
    let mut hash = StableHasher::new(0x504b_4445_4c45_5445);
    hash.write_u64(keys.len() as u64);
    for key in keys {
        hash.write_bytes(key);
    }
    hash.finish()
}

fn stable_hash_delete_patch(patch: &DeletePatchRef) -> u64 {
    let mut hash = StableHasher::new(0x4450_4154_4348_5631);
    match patch {
        DeletePatchRef::Inline(inline) => {
            hash.write_u8(0);
            hash.write_u8(match inline.encoding {
                DeletePatchEncoding::GroupedRowOffsetDeltaV1 => 1,
            });
            hash.write_u32(inline.row_count);
            hash.write_u64(inline.groups.len() as u64);
            for group in &inline.groups {
                hash.write_u64(group.rowset_id);
                hash.write_u64(group.segments.len() as u64);
                for segment in &group.segments {
                    hash.write_u32(segment.segment_id);
                    hash.write_u64(segment.row_offsets_delta.len() as u64);
                    for delta in &segment.row_offsets_delta {
                        hash.write_u32(*delta);
                    }
                }
            }
        }
        DeletePatchRef::Artifact(reference) => {
            hash.write_u8(1);
            hash.write_u8(match reference.namespace {
                ArtifactNamespace::CanonicalRowset => 1,
                ArtifactNamespace::Staged => 2,
                ArtifactNamespace::DeletePatch => 3,
            });
            hash.write_u64(reference.locator.len() as u64);
            for component in &reference.locator {
                hash.write_bytes(component.as_bytes());
            }
        }
    }
    hash.finish()
}

struct StableHasher {
    state: u64,
}

impl StableHasher {
    const FNV_PRIME: u64 = 0x1000_0000_01b3;

    fn new(seed: u64) -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325 ^ seed,
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.state ^= value as u64;
        self.state = self.state.wrapping_mul(Self::FNV_PRIME);
    }

    fn write_u32(&mut self, value: u32) {
        self.write_raw_bytes(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write_raw_bytes(&value.to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        self.write_raw_bytes(bytes);
    }

    fn write_raw_bytes(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.write_u8(byte);
        }
    }

    fn finish(self) -> u64 {
        self.state
    }
}

impl DeletePatchInline {
    pub fn decode_row_refs(&self) -> Result<Vec<(u64, u32, u32)>> {
        match self.encoding {
            DeletePatchEncoding::GroupedRowOffsetDeltaV1 => {
                let mut locations = Vec::with_capacity(self.row_count as usize);
                for group in &self.groups {
                    for segment in &group.segments {
                        let mut previous = 0u32;
                        let mut first = true;
                        for delta in &segment.row_offsets_delta {
                            let row_offset = if first {
                                first = false;
                                *delta
                            } else {
                                previous.checked_add(*delta).ok_or_else(|| {
                                    paro_error::serialization_error(
                                        "delete patch row offset delta overflow",
                                    )
                                })?
                            };
                            previous = row_offset;
                            locations.push((group.rowset_id, segment.segment_id, row_offset));
                        }
                    }
                }
                Ok(locations)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletePatchGroup {
    pub rowset_id: u64,
    pub segments: Vec<DeletePatchSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletePatchSegment {
    pub segment_id: u32,
    pub row_offsets_delta: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DeletePatchArtifactFile {
    format_version: u32,
    patch: DeletePatchInline,
}

pub fn encode_delete_patch_artifact_bytes(patch: &DeletePatchInline) -> Result<Vec<u8>> {
    let payload = DeletePatchArtifactFile {
        format_version: DELETE_PATCH_ARTIFACT_VERSION,
        patch: patch.clone(),
    };
    let encoded = bincode::serialize(&payload)
        .map_err(|err| paro_error::serialization_error(err.to_string()))?;
    let mut out = Vec::with_capacity(DELETE_PATCH_ARTIFACT_MAGIC.len() + encoded.len());
    out.extend_from_slice(&DELETE_PATCH_ARTIFACT_MAGIC);
    out.extend_from_slice(&encoded);
    Ok(out)
}

pub fn decode_delete_patch_artifact_bytes(bytes: &[u8]) -> Result<DeletePatchInline> {
    if bytes.len() < DELETE_PATCH_ARTIFACT_MAGIC.len()
        || bytes[..DELETE_PATCH_ARTIFACT_MAGIC.len()] != DELETE_PATCH_ARTIFACT_MAGIC
    {
        return Err(paro_error::data_corrupted(
            "delete patch artifact missing current magic header",
        ));
    }
    let artifact: DeletePatchArtifactFile =
        bincode::deserialize(&bytes[DELETE_PATCH_ARTIFACT_MAGIC.len()..])
            .map_err(|err| paro_error::serialization_error(err.to_string()))?;
    if artifact.format_version != DELETE_PATCH_ARTIFACT_VERSION {
        return Err(paro_error::data_corrupted(format!(
            "unsupported delete patch artifact version {}",
            artifact.format_version
        )));
    }
    Ok(artifact.patch)
}

fn path_components(path: &Path) -> Vec<String> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_tablet_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "paro_artifact_ref_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn sample_patch() -> DeletePatchInline {
        DeletePatchInline {
            encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
            row_count: 3,
            groups: vec![
                DeletePatchGroup {
                    rowset_id: 11,
                    segments: vec![DeletePatchSegment {
                        segment_id: 0,
                        row_offsets_delta: vec![3, 4],
                    }],
                },
                DeletePatchGroup {
                    rowset_id: 12,
                    segments: vec![DeletePatchSegment {
                        segment_id: 1,
                        row_offsets_delta: vec![9],
                    }],
                },
            ],
        }
    }

    #[test]
    fn delete_patch_artifact_bytes_roundtrip() {
        let patch = sample_patch();
        let encoded = encode_delete_patch_artifact_bytes(&patch).unwrap();
        let decoded = decode_delete_patch_artifact_bytes(&encoded).unwrap();
        assert_eq!(decoded, patch);
        assert_eq!(
            decoded.decode_row_refs().unwrap(),
            vec![(11, 0, 3), (11, 0, 7), (12, 1, 9)]
        );
    }

    #[test]
    fn artifact_delete_patch_ref_decodes_rows_from_file() {
        let tablet_dir = temp_tablet_dir();
        let path = tablet_dir
            .join("_delete_patch")
            .join("txn_7")
            .join("patch.bin");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            encode_delete_patch_artifact_bytes(&sample_patch()).unwrap(),
        )
        .unwrap();

        let decoded = DeletePatchRef::Artifact(ArtifactRef {
            namespace: ArtifactNamespace::DeletePatch,
            locator: vec!["txn_7".to_string(), "patch.bin".to_string()],
        })
        .decode_row_refs_for_tablet(&tablet_dir)
        .unwrap();

        assert_eq!(decoded, vec![(11, 0, 3), (11, 0, 7), (12, 1, 9)]);
        let _ = std::fs::remove_dir_all(&tablet_dir);
    }

    #[test]
    fn artifact_ref_roundtrips_tablet_namespace_paths() {
        let tablet_dir = temp_tablet_dir();
        let canonical = tablet_dir.join("rowsets").join("rowset_9");
        let staged = tablet_dir
            .join("_staged")
            .join("txn")
            .join("txn_7")
            .join("rowset_9");
        let delete_patch = tablet_dir
            .join("_delete_patch")
            .join("txn_7")
            .join("patch_0.bin");

        assert_eq!(
            ArtifactRef::from_tablet_path(&tablet_dir, &canonical).unwrap(),
            ArtifactRef {
                namespace: ArtifactNamespace::CanonicalRowset,
                locator: vec!["rowset_9".to_string()],
            }
        );
        assert_eq!(
            ArtifactRef::from_tablet_path(&tablet_dir, &staged).unwrap(),
            ArtifactRef {
                namespace: ArtifactNamespace::Staged,
                locator: vec![
                    "txn".to_string(),
                    "txn_7".to_string(),
                    "rowset_9".to_string()
                ],
            }
        );
        let delete_patch_ref = ArtifactRef::from_tablet_path(&tablet_dir, &delete_patch).unwrap();
        assert_eq!(
            delete_patch_ref,
            ArtifactRef {
                namespace: ArtifactNamespace::DeletePatch,
                locator: vec!["txn_7".to_string(), "patch_0.bin".to_string()],
            }
        );
        assert_eq!(
            delete_patch_ref.resolve_for_tablet(&tablet_dir),
            delete_patch
        );
        let _ = std::fs::remove_dir_all(&tablet_dir);
    }
}
