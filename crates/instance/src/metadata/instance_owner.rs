use crate::metadata::instance_layout::InstanceLayout;
use fs2::FileExt;
use parking_lot::Mutex;
use paro_common::error::{self as paro_error, Result};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

static OWNED_INSTANCE_ROOTS: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Debug)]
pub(crate) struct InstanceOwnerGuard {
    owned_root: PathBuf,
    lock_file: File,
}

impl InstanceOwnerGuard {
    pub(crate) fn acquire(layout: &InstanceLayout) -> Result<Self> {
        let owned_root = ownership_identity_root(layout.root());
        {
            let mut owned_roots = OWNED_INSTANCE_ROOTS.lock();
            if !owned_roots.insert(owned_root.clone()) {
                return Err(paro_error::cannot_connect_now().detail(format!(
                    "Instance root {} is already owned by this process",
                    layout.root().display()
                )));
            }
        }

        match Self::acquire_file_lock(layout) {
            Ok((lock_path, mut lock_file)) => {
                Self::write_lock_owner_metadata(&mut lock_file, layout.root(), &lock_path)?;
                Ok(Self {
                    owned_root,
                    lock_file,
                })
            }
            Err(err) => {
                OWNED_INSTANCE_ROOTS.lock().remove(&owned_root);
                Err(err)
            }
        }
    }

    fn acquire_file_lock(layout: &InstanceLayout) -> Result<(PathBuf, File)> {
        let lock_path = layout.owner_lock_path();
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create instance owner directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to open instance owner lock file {}: {}",
                    lock_path.display(),
                    e
                ))
            })?;

        lock_file.try_lock_exclusive().map_err(|e| match e.kind() {
            ErrorKind::WouldBlock => paro_error::cannot_connect_now().detail(format!(
                "Instance root {} is already owned by another process (lock file: {})",
                layout.root().display(),
                lock_path.display()
            )),
            _ => paro_error::io_error(format!(
                "Failed to lock instance owner file {}: {}",
                lock_path.display(),
                e
            )),
        })?;

        Ok((lock_path, lock_file))
    }

    fn write_lock_owner_metadata(
        lock_file: &mut File,
        instance_root: &Path,
        lock_path: &Path,
    ) -> Result<()> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        lock_file.set_len(0).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to truncate instance owner lock file {}: {}",
                lock_path.display(),
                e
            ))
        })?;
        lock_file
            .write_all(
                format!(
                    "pid={}\ninstance_root={}\nacquired_at_ms={}\n",
                    std::process::id(),
                    instance_root.display(),
                    now_ms
                )
                .as_bytes(),
            )
            .map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to write instance owner metadata {}: {}",
                    lock_path.display(),
                    e
                ))
            })?;
        lock_file.sync_all().map_err(|e| {
            paro_error::io_error(format!(
                "Failed to fsync instance owner lock file {}: {}",
                lock_path.display(),
                e
            ))
        })
    }
}

impl Drop for InstanceOwnerGuard {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
        OWNED_INSTANCE_ROOTS.lock().remove(&self.owned_root);
    }
}

fn ownership_identity_root(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
