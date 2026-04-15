// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};

/// Logical on-disk layout for a persistent instance root.
///
/// This type only performs lexical path normalization and never touches the
/// filesystem directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceLayout {
    root: PathBuf,
}

impl InstanceLayout {
    pub fn new(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let root = if root.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            root.to_path_buf()
        };
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn instance_dir(&self) -> PathBuf {
        self.root.join("instance")
    }

    pub fn meta_dir(&self) -> PathBuf {
        self.instance_dir().join("meta")
    }

    pub fn owner_lock_path(&self) -> PathBuf {
        self.instance_dir().join("owner.lock")
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.meta_dir().join("catalog.json")
    }

    pub fn run_state_path(&self) -> PathBuf {
        self.meta_dir().join("run_state.json")
    }

    pub fn databases_dir(&self) -> PathBuf {
        self.root.join("databases")
    }

    pub fn managed_database_dir(&self, database_id: u64) -> PathBuf {
        self.databases_dir().join(format!("db-{database_id}"))
    }
}

#[cfg(test)]
mod tests {
    use super::InstanceLayout;
    use std::path::PathBuf;

    #[test]
    fn empty_root_is_normalized_lexically() {
        let layout = InstanceLayout::new("");
        assert_eq!(layout.root(), PathBuf::from("."));
    }

    #[test]
    fn persistent_paths_follow_instance_layout_contract() {
        let layout = InstanceLayout::new("/tmp/paro");
        assert_eq!(layout.instance_dir(), PathBuf::from("/tmp/paro/instance"));
        assert_eq!(layout.meta_dir(), PathBuf::from("/tmp/paro/instance/meta"));
        assert_eq!(
            layout.owner_lock_path(),
            PathBuf::from("/tmp/paro/instance/owner.lock")
        );
        assert_eq!(
            layout.catalog_path(),
            PathBuf::from("/tmp/paro/instance/meta/catalog.json")
        );
        assert_eq!(
            layout.run_state_path(),
            PathBuf::from("/tmp/paro/instance/meta/run_state.json")
        );
        assert_eq!(layout.databases_dir(), PathBuf::from("/tmp/paro/databases"));
        assert_eq!(
            layout.managed_database_dir(7),
            PathBuf::from("/tmp/paro/databases/db-7")
        );
    }
}
