// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::handle::DbState;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

/// Mutable lifecycle state for an attached database.
pub struct DatabaseState {
    state: RwLock<DbState>,
    is_closed: AtomicBool,
    close_lock: Mutex<()>,
}

impl DatabaseState {
    pub fn new(initial_state: DbState) -> Self {
        Self {
            state: RwLock::new(initial_state),
            is_closed: AtomicBool::new(false),
            close_lock: Mutex::new(()),
        }
    }

    pub fn get(&self) -> DbState {
        *self.state.read()
    }

    pub fn is_ready(&self) -> bool {
        self.get() == DbState::Ready
    }

    pub fn set_ready(&self) {
        *self.state.write() = DbState::Ready;
    }

    pub fn set_dropping(&self) {
        *self.state.write() = DbState::Dropping;
    }

    pub fn set_dropped(&self) {
        *self.state.write() = DbState::Dropped;
    }

    pub fn try_mark_dropping(&self) -> bool {
        let mut state = self.state.write();
        if *state == DbState::Ready {
            *state = DbState::Dropping;
            true
        } else {
            false
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    pub fn mark_closed(&self) -> bool {
        !self.is_closed.swap(true, Ordering::AcqRel)
    }

    pub fn close_guard(&self) -> MutexGuard<'_, ()> {
        self.close_lock.lock()
    }
}
