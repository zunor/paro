// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::storage_identity::{DatabaseStorageIdentity, DATABASE_STORAGE_IDENTITY_KEY};
use crate::storage_manager::StorageManager;
use bincode::Options;
use crc32fast::hash as crc32;
use paro_common::checkpoint::{
    CheckpointCurrentPointer, CheckpointDatabaseIdentity, CheckpointFrontier, CheckpointManifest,
    JournalTailRef, RecoverySummary, RetentionFloor, SnapshotBundleRef,
    CHECKPOINT_CURRENT_POINTER_FORMAT_VERSION, CHECKPOINT_MANIFEST_FORMAT_VERSION,
};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(test, debug_assertions))]
use std::sync::{LazyLock, Mutex};

const CHECKPOINT_ROOT_DIR: &str = "checkpoints";
const MANIFESTS_DIR: &str = "manifests";
const BUNDLES_DIR: &str = "bundles";
const CURRENT_FILE_NAME: &str = "CURRENT";
const MANIFEST_MAGIC: [u8; 4] = *b"CPMF";
const BUNDLE_MAGIC: [u8; 4] = *b"CPKB";
const CURRENT_MAGIC: [u8; 4] = *b"CPCU";

#[cfg(any(test, debug_assertions))]
static FAIL_MANIFEST_RENAME: LazyLock<Mutex<Vec<ManifestFailpoint>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));
#[cfg(any(test, debug_assertions))]
static FAIL_MANIFEST_PARENT_SYNC: LazyLock<Mutex<Vec<ManifestFailpoint>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

#[cfg(any(test, debug_assertions))]
#[derive(Debug, Clone)]
struct ManifestFailpoint {
    remaining_calls: usize,
    target_path: Option<PathBuf>,
}

fn checkpoint_bincode() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

#[derive(Debug, Clone)]
pub struct StagedCheckpointPublish {
    pub checkpoint_id: u64,
    pub previous_checkpoint_id: Option<u64>,
    pub created_at_micros: u64,
    pub database_identity: CheckpointDatabaseIdentity,
    pub staging_dir: PathBuf,
    bundle_refs: Vec<SnapshotBundleRef>,
}

impl StagedCheckpointPublish {
    pub fn bundle_refs(&self) -> &[SnapshotBundleRef] {
        &self.bundle_refs
    }
}

#[derive(Debug, Clone)]
pub struct ManifestStore {
    checkpoint_root: PathBuf,
    manifests_dir: PathBuf,
    bundles_dir: PathBuf,
}

impl ManifestStore {
    pub fn open_for_storage(storage: &dyn StorageManager) -> anyhow::Result<Option<Self>> {
        let Some(root) = storage_root_from_path(storage.get_path()) else {
            return Ok(None);
        };
        Self::open_database_root(root).map(Some)
    }

    pub fn open_database_root(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        let checkpoint_root = root.join(CHECKPOINT_ROOT_DIR);
        let manifests_dir = checkpoint_root.join(MANIFESTS_DIR);
        let bundles_dir = checkpoint_root.join(BUNDLES_DIR);

        fs::create_dir_all(&manifests_dir)?;
        fs::create_dir_all(&bundles_dir)?;

        Ok(Self {
            checkpoint_root,
            manifests_dir,
            bundles_dir,
        })
    }

    pub fn checkpoint_root(&self) -> &Path {
        &self.checkpoint_root
    }

    pub fn load_database_identity(
        storage: &dyn StorageManager,
    ) -> anyhow::Result<CheckpointDatabaseIdentity> {
        let metadata_store = storage
            .get_metadata_store_arc()
            .ok_or_else(|| anyhow::anyhow!("MetadataStore unavailable for checkpoint identity"))?;
        let payload = metadata_store
            .get(DATABASE_STORAGE_IDENTITY_KEY)
            .map_err(|e| anyhow::anyhow!(e))?
            .ok_or_else(|| anyhow::anyhow!("Storage identity missing from MetadataStore"))?;
        let identity: DatabaseStorageIdentity = serde_json::from_slice(&payload)?;
        identity.validate()?;
        Ok(CheckpointDatabaseIdentity {
            format_version: identity.format_version,
            database_id: identity.database_id,
            db_identifier: identity.db_identifier.to_vec(),
            created_at_ms: identity.created_at_ms,
        })
    }

    pub fn sweep_orphan_staging_dirs(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut removed = Vec::new();
        if !self.bundles_dir.exists() {
            return Ok(removed);
        }

        for entry in fs::read_dir(&self.bundles_dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("_staging_") {
                continue;
            }
            fs::remove_dir_all(&path)?;
            sync_parent_dir(&path)?;
            removed.push(path);
        }

        Ok(removed)
    }

    pub fn begin_publish(
        &self,
        database_identity: CheckpointDatabaseIdentity,
    ) -> anyhow::Result<StagedCheckpointPublish> {
        self.sweep_orphan_staging_dirs()?;

        let previous_checkpoint_id = self.read_current_manifest()?.map(|m| m.checkpoint_id);
        let checkpoint_id = previous_checkpoint_id.unwrap_or(0).saturating_add(1);
        let staging_dir = self
            .bundles_dir
            .join(format!("_staging_{checkpoint_id:020}"));

        if staging_dir.exists() {
            fs::remove_dir_all(&staging_dir)?;
        }
        fs::create_dir_all(&staging_dir)?;
        sync_parent_dir(&staging_dir)?;

        Ok(StagedCheckpointPublish {
            checkpoint_id,
            previous_checkpoint_id,
            created_at_micros: current_timestamp_micros(),
            database_identity,
            staging_dir,
            bundle_refs: Vec::new(),
        })
    }

    pub fn stage_raw_bundle(
        &self,
        staged: &mut StagedCheckpointPublish,
        file_name: &str,
        kind: paro_common::checkpoint::BundleKind,
        format_version: u32,
        payload: &[u8],
        base_checkpoint_id: Option<u64>,
    ) -> anyhow::Result<()> {
        let staging_path = staged.staging_dir.join(file_name);
        let encoded = encode_envelope(BUNDLE_MAGIC, format_version, payload);
        write_durable_file(&staging_path, &encoded)?;

        staged.bundle_refs.push(SnapshotBundleRef {
            kind,
            locator: format!("bundles/{:020}/{}", staged.checkpoint_id, file_name),
            size_bytes: payload.len() as u64,
            checksum_crc32c: crc32(payload),
            format_version,
            base_checkpoint_id,
        });

        Ok(())
    }

    pub fn stage_bundle<T: serde::Serialize>(
        &self,
        staged: &mut StagedCheckpointPublish,
        file_name: &str,
        kind: paro_common::checkpoint::BundleKind,
        format_version: u32,
        payload: &T,
        base_checkpoint_id: Option<u64>,
    ) -> anyhow::Result<()> {
        let bytes = checkpoint_bincode().serialize(payload)?;
        self.stage_raw_bundle(
            staged,
            file_name,
            kind,
            format_version,
            &bytes,
            base_checkpoint_id,
        )
    }

    pub fn publish_manifest(
        &self,
        staged: StagedCheckpointPublish,
        frontier: CheckpointFrontier,
        bootstrap: RecoverySummary,
        journal: JournalTailRef,
        retention_floor: RetentionFloor,
    ) -> anyhow::Result<CheckpointManifest> {
        let final_bundle_dir = self.bundle_dir(staged.checkpoint_id);
        if final_bundle_dir.exists() {
            anyhow::bail!(
                "checkpoint bundle directory already exists: {}",
                final_bundle_dir.display()
            );
        }

        fs::rename(&staged.staging_dir, &final_bundle_dir)?;
        sync_parent_dir(&final_bundle_dir)?;

        let manifest = CheckpointManifest {
            format_version: CHECKPOINT_MANIFEST_FORMAT_VERSION,
            checkpoint_id: staged.checkpoint_id,
            previous_checkpoint_id: staged.previous_checkpoint_id,
            created_at_micros: staged.created_at_micros,
            database_identity: staged.database_identity,
            frontier,
            bootstrap,
            journal,
            bundle_refs: staged.bundle_refs,
            retention_floor,
        };

        let manifest_payload = checkpoint_bincode().serialize(&manifest)?;
        let manifest_path = self.manifest_path(manifest.checkpoint_id);
        let manifest_bytes =
            encode_envelope(MANIFEST_MAGIC, manifest.format_version, &manifest_payload);
        write_durable_file(&manifest_path, &manifest_bytes)?;

        let current = CheckpointCurrentPointer {
            format_version: CHECKPOINT_CURRENT_POINTER_FORMAT_VERSION,
            checkpoint_id: manifest.checkpoint_id,
            manifest_locator: self.relative_to_root(&manifest_path)?,
            manifest_checksum_crc32c: crc32(&manifest_payload),
        };
        let current_payload = checkpoint_bincode().serialize(&current)?;
        let current_bytes =
            encode_envelope(CURRENT_MAGIC, current.format_version, &current_payload);
        write_durable_file(&self.current_path(), &current_bytes)?;

        Ok(manifest)
    }

    pub fn read_current_manifest(&self) -> anyhow::Result<Option<CheckpointManifest>> {
        let current_path = self.current_path();
        if !current_path.exists() {
            return Ok(None);
        }

        let (pointer_version, pointer_payload, pointer_checksum) =
            decode_envelope(&current_path, CURRENT_MAGIC)?;
        let current: CheckpointCurrentPointer =
            checkpoint_bincode().deserialize(&pointer_payload)?;
        if current.format_version != pointer_version {
            anyhow::bail!(
                "CURRENT pointer format version {} does not match envelope {}",
                current.format_version,
                pointer_version
            );
        }
        if crc32(&pointer_payload) != pointer_checksum {
            anyhow::bail!("CURRENT pointer checksum mismatch");
        }

        let manifest_path = self.checkpoint_root.join(&current.manifest_locator);
        let (manifest_version, manifest_payload, manifest_checksum) =
            decode_envelope(&manifest_path, MANIFEST_MAGIC)?;
        if current.manifest_checksum_crc32c != manifest_checksum {
            anyhow::bail!(
                "CURRENT pointer checksum {} does not match manifest checksum {}",
                current.manifest_checksum_crc32c,
                manifest_checksum
            );
        }
        let manifest: CheckpointManifest = checkpoint_bincode().deserialize(&manifest_payload)?;
        if manifest.format_version != manifest_version {
            anyhow::bail!(
                "Manifest format version {} does not match envelope {}",
                manifest.format_version,
                manifest_version
            );
        }
        if manifest.checkpoint_id != current.checkpoint_id {
            anyhow::bail!(
                "CURRENT checkpoint_id {} does not match manifest {}",
                current.checkpoint_id,
                manifest.checkpoint_id
            );
        }
        Ok(Some(manifest))
    }

    pub fn read_manifest(&self, checkpoint_id: u64) -> anyhow::Result<Option<CheckpointManifest>> {
        let manifest_path = self.manifest_path(checkpoint_id);
        if !manifest_path.exists() {
            return Ok(None);
        }
        Ok(Some(self.read_manifest_file(&manifest_path)?))
    }

    pub fn list_manifests(&self) -> anyhow::Result<Vec<CheckpointManifest>> {
        let mut checkpoint_ids = Vec::new();
        if !self.manifests_dir.exists() {
            return Ok(Vec::new());
        }

        for entry in fs::read_dir(&self.manifests_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.file_name().and_then(|name| name.to_str()) == Some(CURRENT_FILE_NAME) {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Ok(checkpoint_id) = stem.parse::<u64>() else {
                continue;
            };
            checkpoint_ids.push(checkpoint_id);
        }

        checkpoint_ids.sort_unstable();
        let mut manifests = Vec::with_capacity(checkpoint_ids.len());
        for checkpoint_id in checkpoint_ids {
            if let Some(manifest) = self.read_manifest(checkpoint_id)? {
                manifests.push(manifest);
            }
        }
        Ok(manifests)
    }

    pub fn read_bundle_payload(&self, bundle: &SnapshotBundleRef) -> anyhow::Result<Vec<u8>> {
        let bundle_path = self.checkpoint_root.join(&bundle.locator);
        let (format_version, payload, checksum) = decode_envelope(&bundle_path, BUNDLE_MAGIC)?;
        if format_version != bundle.format_version {
            anyhow::bail!(
                "Bundle format version {} does not match ref {}",
                format_version,
                bundle.format_version
            );
        }
        if checksum != bundle.checksum_crc32c {
            anyhow::bail!(
                "Bundle checksum {} does not match ref {}",
                checksum,
                bundle.checksum_crc32c
            );
        }
        if payload.len() as u64 != bundle.size_bytes {
            anyhow::bail!(
                "Bundle size {} does not match ref {}",
                payload.len(),
                bundle.size_bytes
            );
        }
        Ok(payload)
    }

    fn current_path(&self) -> PathBuf {
        self.manifests_dir.join(CURRENT_FILE_NAME)
    }

    pub(crate) fn manifest_path(&self, checkpoint_id: u64) -> PathBuf {
        self.manifests_dir.join(format!("{checkpoint_id:020}.bin"))
    }

    pub(crate) fn bundle_dir(&self, checkpoint_id: u64) -> PathBuf {
        self.bundles_dir.join(format!("{checkpoint_id:020}"))
    }

    fn read_manifest_file(&self, manifest_path: &Path) -> anyhow::Result<CheckpointManifest> {
        let (manifest_version, manifest_payload, _) =
            decode_envelope(manifest_path, MANIFEST_MAGIC)?;
        let manifest: CheckpointManifest = checkpoint_bincode().deserialize(&manifest_payload)?;
        if manifest.format_version != manifest_version {
            anyhow::bail!(
                "Manifest format version {} does not match envelope {}",
                manifest.format_version,
                manifest_version
            );
        }
        Ok(manifest)
    }

    fn relative_to_root(&self, path: &Path) -> anyhow::Result<String> {
        let relative = path.strip_prefix(&self.checkpoint_root).map_err(|err| {
            anyhow::anyhow!(
                "path {} is not under checkpoint root {}: {}",
                path.display(),
                self.checkpoint_root.display(),
                err
            )
        })?;
        Ok(relative.to_string_lossy().replace('\\', "/"))
    }
}

fn storage_root_from_path(path: &str) -> Option<PathBuf> {
    let base_path = path.split('?').next().unwrap_or(path);
    if base_path.is_empty() || base_path == ":memory:" {
        None
    } else {
        Some(PathBuf::from(base_path))
    }
}

fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

fn encode_envelope(magic: [u8; 4], format_version: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 4 + 8 + 4 + payload.len());
    out.extend_from_slice(&magic);
    out.extend_from_slice(&format_version.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn decode_envelope(path: &Path, expected_magic: [u8; 4]) -> anyhow::Result<(u32, Vec<u8>, u32)> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if magic != expected_magic {
        anyhow::bail!("invalid checkpoint envelope magic for {}", path.display());
    }

    let mut version_buf = [0u8; 4];
    file.read_exact(&mut version_buf)?;
    let format_version = u32::from_le_bytes(version_buf);

    let mut len_buf = [0u8; 8];
    file.read_exact(&mut len_buf)?;
    let payload_len = u64::from_le_bytes(len_buf) as usize;

    let mut checksum_buf = [0u8; 4];
    file.read_exact(&mut checksum_buf)?;
    let checksum = u32::from_le_bytes(checksum_buf);

    let mut payload = vec![0u8; payload_len];
    file.read_exact(&mut payload)?;
    if crc32(&payload) != checksum {
        anyhow::bail!(
            "checkpoint envelope checksum mismatch for {}",
            path.display()
        );
    }

    let mut trailing = Vec::new();
    file.read_to_end(&mut trailing)?;
    if !trailing.is_empty() {
        anyhow::bail!(
            "checkpoint envelope has trailing bytes for {}",
            path.display()
        );
    }

    Ok((format_version, payload, checksum))
}

fn write_durable_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!("checkpoint path {} has no parent directory", path.display())
    })?;
    fs::create_dir_all(parent)?;

    let tmp_path = next_tmp_path(path);
    let mut file = File::create(&tmp_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);

    #[cfg(any(test, debug_assertions))]
    if should_fail_for_path(&FAIL_MANIFEST_RENAME, path) {
        let _ = fs::remove_file(&tmp_path);
        anyhow::bail!(
            "Simulated checkpoint rename failure while publishing {}",
            path.display()
        );
    }

    fs::rename(&tmp_path, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

fn next_tmp_path(target: &Path) -> PathBuf {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let base_name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "checkpoint".to_string());
    target.with_file_name(format!("{base_name}.tmp-{pid}-{nanos}"))
}

fn sync_parent_dir(path: &Path) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    #[cfg(any(test, debug_assertions))]
    if should_fail_for_path(&FAIL_MANIFEST_PARENT_SYNC, path) {
        anyhow::bail!(
            "Simulated checkpoint parent directory fsync failure for {}",
            parent.display()
        );
    }
    let dir = File::open(parent)?;
    dir.sync_all()?;
    Ok(())
}

#[cfg(any(test, debug_assertions))]
fn should_fail_for_path(state: &LazyLock<Mutex<Vec<ManifestFailpoint>>>, path: &Path) -> bool {
    let mut armed = state.lock().expect("manifest failpoint mutex poisoned");
    let mut matched = false;
    armed.retain_mut(|failpoint| {
        let target_matches = failpoint
            .target_path
            .as_ref()
            .map(|target| target == path)
            .unwrap_or(true);
        if !target_matches {
            return true;
        }
        if failpoint.remaining_calls > 1 {
            failpoint.remaining_calls -= 1;
            return true;
        }
        matched = true;
        false
    });
    matched
}

#[cfg(any(test, debug_assertions))]
pub mod testing {
    use super::{ManifestFailpoint, FAIL_MANIFEST_PARENT_SYNC, FAIL_MANIFEST_RENAME};
    use std::path::Path;

    pub fn arm_manifest_rename_failure_for_path_on_nth_call(
        path: impl AsRef<Path>,
        nth_call: usize,
    ) {
        FAIL_MANIFEST_RENAME
            .lock()
            .expect("manifest rename failpoint mutex poisoned")
            .push(ManifestFailpoint {
                remaining_calls: nth_call.max(1),
                target_path: Some(path.as_ref().to_path_buf()),
            });
    }

    pub fn arm_manifest_parent_sync_failure_for_path_on_nth_call(
        path: impl AsRef<Path>,
        nth_call: usize,
    ) {
        FAIL_MANIFEST_PARENT_SYNC
            .lock()
            .expect("manifest parent-sync failpoint mutex poisoned")
            .push(ManifestFailpoint {
                remaining_calls: nth_call.max(1),
                target_path: Some(path.as_ref().to_path_buf()),
            });
    }
}

#[cfg(test)]
mod tests {
    use super::testing::arm_manifest_rename_failure_for_path_on_nth_call;
    use super::*;
    use paro_common::checkpoint::{
        BundleKind, CheckpointFrontier, RecoverySummary, RetentionFloor,
        CATALOG_BUNDLE_FORMAT_VERSION,
    };
    use tempfile::tempdir;

    fn test_identity() -> CheckpointDatabaseIdentity {
        CheckpointDatabaseIdentity {
            format_version: 1,
            database_id: 7,
            db_identifier: vec![1, 2, 3, 4],
            created_at_ms: 42,
        }
    }

    #[test]
    fn manifest_store_roundtrip_publishes_current_and_bundles() {
        let temp = tempdir().expect("tempdir should succeed");
        let store = ManifestStore::open_database_root(temp.path()).expect("open store");
        let mut staged = store.begin_publish(test_identity()).expect("begin publish");
        store
            .stage_raw_bundle(
                &mut staged,
                "catalog.bin",
                BundleKind::Catalog,
                CATALOG_BUNDLE_FORMAT_VERSION,
                b"catalog-payload",
                None,
            )
            .expect("stage bundle");

        let manifest = store
            .publish_manifest(
                staged,
                CheckpointFrontier {
                    checkpoint_lsn: 9,
                    checkpoint_commit_id: 10,
                    checkpoint_maintenance_id: 11,
                },
                RecoverySummary {
                    max_lsn: 9,
                    max_commit_id: 10,
                    max_maintenance_id: 11,
                    max_catalog_commit_id: 12,
                    max_seen_object_id: 13,
                },
                JournalTailRef {
                    replay_from_segment_id: 0,
                    replay_from_lsn: 10,
                },
                RetentionFloor {
                    checkpoint_lsn: 9,
                    manual_keep_from_lsn: None,
                    backup_floor_lsn: None,
                    replication_floor_lsn: None,
                    pitr_floor_lsn: None,
                },
            )
            .expect("publish manifest");

        let current = store
            .read_current_manifest()
            .expect("read current manifest")
            .expect("manifest should exist");
        assert_eq!(current, manifest);

        let payload = store
            .read_bundle_payload(&manifest.bundle_refs[0])
            .expect("read bundle payload");
        assert_eq!(payload, b"catalog-payload");
    }

    #[test]
    fn sweep_orphan_staging_dirs_removes_stale_directories() {
        let temp = tempdir().expect("tempdir should succeed");
        let store = ManifestStore::open_database_root(temp.path()).expect("open store");
        let stale = store.bundles_dir.join("_staging_00000000000000000001");
        fs::create_dir_all(&stale).expect("create stale dir");
        fs::write(stale.join("bundle.bin"), b"payload").expect("write stale payload");

        let removed = store
            .sweep_orphan_staging_dirs()
            .expect("sweep should succeed");
        assert_eq!(removed.len(), 1);
        assert!(!stale.exists());
    }

    #[test]
    fn failed_current_publish_keeps_previous_committed_manifest() {
        let temp = tempdir().expect("tempdir should succeed");
        let store = ManifestStore::open_database_root(temp.path()).expect("open store");

        let mut first = store
            .begin_publish(test_identity())
            .expect("begin first publish");
        store
            .stage_raw_bundle(
                &mut first,
                "catalog.bin",
                BundleKind::Catalog,
                CATALOG_BUNDLE_FORMAT_VERSION,
                b"v1",
                None,
            )
            .expect("stage first bundle");
        let first_manifest = store
            .publish_manifest(
                first,
                CheckpointFrontier {
                    checkpoint_lsn: 1,
                    checkpoint_commit_id: 1,
                    checkpoint_maintenance_id: 0,
                },
                RecoverySummary {
                    max_lsn: 1,
                    max_commit_id: 1,
                    max_maintenance_id: 0,
                    max_catalog_commit_id: 1,
                    max_seen_object_id: 1,
                },
                JournalTailRef {
                    replay_from_segment_id: 0,
                    replay_from_lsn: 2,
                },
                RetentionFloor {
                    checkpoint_lsn: 1,
                    manual_keep_from_lsn: None,
                    backup_floor_lsn: None,
                    replication_floor_lsn: None,
                    pitr_floor_lsn: None,
                },
            )
            .expect("publish first manifest");

        let mut second = store
            .begin_publish(test_identity())
            .expect("begin second publish");
        store
            .stage_raw_bundle(
                &mut second,
                "catalog.bin",
                BundleKind::Catalog,
                CATALOG_BUNDLE_FORMAT_VERSION,
                b"v2",
                None,
            )
            .expect("stage second bundle");
        arm_manifest_rename_failure_for_path_on_nth_call(store.current_path(), 1);
        let err = store.publish_manifest(
            second,
            CheckpointFrontier {
                checkpoint_lsn: 2,
                checkpoint_commit_id: 2,
                checkpoint_maintenance_id: 0,
            },
            RecoverySummary {
                max_lsn: 2,
                max_commit_id: 2,
                max_maintenance_id: 0,
                max_catalog_commit_id: 2,
                max_seen_object_id: 2,
            },
            JournalTailRef {
                replay_from_segment_id: 0,
                replay_from_lsn: 3,
            },
            RetentionFloor {
                checkpoint_lsn: 2,
                manual_keep_from_lsn: None,
                backup_floor_lsn: None,
                replication_floor_lsn: None,
                pitr_floor_lsn: None,
            },
        );
        assert!(err.is_err(), "second publish should fail");

        let current = store
            .read_current_manifest()
            .expect("read current manifest")
            .expect("current manifest should remain present");
        assert_eq!(current.checkpoint_id, first_manifest.checkpoint_id);
    }

    #[test]
    fn corrupted_current_pointer_checksum_is_rejected() {
        let temp = tempdir().expect("tempdir should succeed");
        let store = ManifestStore::open_database_root(temp.path()).expect("open store");
        let mut staged = store.begin_publish(test_identity()).expect("begin publish");
        store
            .stage_raw_bundle(
                &mut staged,
                "catalog.bin",
                BundleKind::Catalog,
                CATALOG_BUNDLE_FORMAT_VERSION,
                b"catalog-payload",
                None,
            )
            .expect("stage bundle");
        store
            .publish_manifest(
                staged,
                CheckpointFrontier {
                    checkpoint_lsn: 9,
                    checkpoint_commit_id: 10,
                    checkpoint_maintenance_id: 11,
                },
                RecoverySummary {
                    max_lsn: 9,
                    max_commit_id: 10,
                    max_maintenance_id: 11,
                    max_catalog_commit_id: 12,
                    max_seen_object_id: 13,
                },
                JournalTailRef {
                    replay_from_segment_id: 0,
                    replay_from_lsn: 10,
                },
                RetentionFloor {
                    checkpoint_lsn: 9,
                    manual_keep_from_lsn: None,
                    backup_floor_lsn: None,
                    replication_floor_lsn: None,
                    pitr_floor_lsn: None,
                },
            )
            .expect("publish manifest");

        let current_path = store.current_path();
        let mut bytes = fs::read(&current_path).expect("read current pointer");
        let last = bytes
            .last_mut()
            .expect("current pointer file should not be empty");
        *last ^= 0xFF;
        fs::write(&current_path, bytes).expect("rewrite corrupted current pointer");

        assert!(
            store.read_current_manifest().is_err(),
            "checksum mismatch should reject corrupted CURRENT pointer"
        );
    }
}
