// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use parking_lot::{Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceDdlOwner {
    Bootstrap,
    CreateDatabase,
    DropDatabase,
    RenameDatabase,
    Shutdown,
}

#[derive(Debug)]
pub struct InstanceDdlLock {
    owner: Mutex<Option<InstanceDdlOwner>>,
}

impl Default for InstanceDdlLock {
    fn default() -> Self {
        Self::new()
    }
}

impl InstanceDdlLock {
    pub(crate) fn new() -> Self {
        Self {
            owner: Mutex::new(None),
        }
    }

    pub(crate) fn lock(&self, owner: InstanceDdlOwner) -> InstanceDdlGuard<'_> {
        let mut guard = self.owner.lock();
        *guard = Some(owner);
        tracing::trace!(owner = ?owner, "Acquired instance DDL lock");
        InstanceDdlGuard { owner, guard }
    }

    pub fn owner(&self) -> Option<InstanceDdlOwner> {
        *self.owner.lock()
    }
}

pub struct InstanceDdlGuard<'a> {
    owner: InstanceDdlOwner,
    guard: MutexGuard<'a, Option<InstanceDdlOwner>>,
}

impl Drop for InstanceDdlGuard<'_> {
    fn drop(&mut self) {
        *self.guard = None;
        tracing::trace!(owner = ?self.owner, "Released instance DDL lock");
    }
}
