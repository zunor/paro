use crate::meta::TabletMetaManager;
use crate::tablet::TabletState;
use paro_common::effect::CleanupDescriptor;
use paro_common::error::{self as paro_error, Result};
use std::collections::VecDeque;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupBatch {
    pub epoch: u64,
    pub descriptors: Vec<CleanupDescriptor>,
}

#[derive(Debug, Default)]
pub struct DescriptorCleanupQueue {
    pending: VecDeque<CleanupBatch>,
}

impl DescriptorCleanupQueue {
    pub fn enqueue<I>(&mut self, epoch: u64, descriptors: I)
    where
        I: IntoIterator<Item = CleanupDescriptor>,
    {
        let descriptors = descriptors.into_iter().collect::<Vec<_>>();
        if descriptors.is_empty() {
            return;
        }
        self.pending.push_back(CleanupBatch { epoch, descriptors });
    }

    pub fn drain(&mut self) -> Vec<CleanupBatch> {
        self.pending.drain(..).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

pub fn apply_cleanup_descriptor(
    cleanup: &CleanupDescriptor,
    tablet_meta_manager: Option<&TabletMetaManager>,
) -> Result<()> {
    match cleanup {
        CleanupDescriptor::RemoveDirectory {
            path_components,
            recursive,
        } => {
            let path = path_from_components(path_components);
            if !path.exists() {
                return Ok(());
            }
            if *recursive {
                std::fs::remove_dir_all(&path).map_err(|err| {
                    paro_error::internal(format!(
                        "cleanup remove_dir_all {}: {}",
                        path.display(),
                        err
                    ))
                })?;
            } else {
                std::fs::remove_dir(&path).map_err(|err| {
                    paro_error::internal(format!("cleanup remove_dir {}: {}", path.display(), err))
                })?;
            }
            Ok(())
        }
        CleanupDescriptor::ShutdownTablet {
            tablet_id,
            data_dir_components,
            move_to_trash,
        } => {
            if let Some(manager) = tablet_meta_manager {
                if manager.load_tablet_meta(*tablet_id)?.is_some() {
                    manager.update_tablet_state(*tablet_id, TabletState::Shutdown)?;
                }
            }
            crate::tablet::Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(
                &path_from_components(data_dir_components),
                *move_to_trash,
            )
        }
    }
}

pub fn path_from_components(components: &[String]) -> PathBuf {
    let mut iter = components.iter();
    let Some(first) = iter.next() else {
        return PathBuf::new();
    };
    let mut path = PathBuf::from(first);
    for component in iter {
        path.push(component);
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{FileMetadataStore, TabletMetaManager};
    use crate::tablet::{KeysType, TabletColumn, TabletMeta, TabletSchema, TabletState};
    use paro_common::types::LogicalType;
    use std::sync::Arc;

    #[test]
    fn cleanup_queue_preserves_epoch_order() {
        let mut queue = DescriptorCleanupQueue::default();
        queue.enqueue(
            11,
            [CleanupDescriptor::RemoveDirectory {
                path_components: vec!["/tmp".to_string(), "cleanup_a".to_string()],
                recursive: true,
            }],
        );
        queue.enqueue(
            12,
            [CleanupDescriptor::RemoveDirectory {
                path_components: vec!["/tmp".to_string(), "cleanup_b".to_string()],
                recursive: true,
            }],
        );

        let drained = queue.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].epoch, 11);
        assert_eq!(drained[1].epoch, 12);
    }

    #[test]
    fn cleanup_queue_skips_empty_batches() {
        let mut queue = DescriptorCleanupQueue::default();
        queue.enqueue(11, std::iter::empty());
        assert!(queue.is_empty());
    }

    #[test]
    fn shutdown_cleanup_updates_manifest_state_before_sweep() {
        let temp = tempfile::tempdir().unwrap();
        let data_root = temp.path().join("tablets");
        std::fs::create_dir_all(&data_root).unwrap();
        let data_dir = data_root.join("public").join("tablet_7");
        std::fs::create_dir_all(&data_dir).unwrap();

        let store = Arc::new(FileMetadataStore::new(temp.path().join("meta")).unwrap());
        let manager = TabletMetaManager::with_store_and_data_root(store, &data_root);
        let schema = Arc::new(
            TabletSchema::with_version(
                700,
                1,
                vec![TabletColumn::key(0, "id", LogicalType::Integer)],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let mut meta =
            TabletMeta::new(7, 70, 700, schema, data_dir.to_string_lossy().to_string()).unwrap();
        meta.set_tablet_state(TabletState::Running);
        manager.save_tablet_meta(&meta).unwrap();

        apply_cleanup_descriptor(
            &CleanupDescriptor::ShutdownTablet {
                tablet_id: 7,
                data_dir_components: vec![data_dir.to_string_lossy().to_string()],
                move_to_trash: false,
            },
            Some(&manager),
        )
        .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let stored = manager.load_tablet_meta(7).unwrap().unwrap();
        assert_eq!(stored.tablet_state(), TabletState::Shutdown);
        assert!(manager.load_startup_tablets(1).unwrap().is_empty());
    }
}
