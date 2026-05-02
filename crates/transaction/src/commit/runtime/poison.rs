// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime poison handoff shared by finalize callbacks.

use super::{CommitRuntimeInner, CommitRuntimePoison};
use crate::sync::Mutex;
use std::sync::Arc;

#[derive(Default)]
pub(super) struct RuntimePoisonCell {
    runtime: Mutex<Option<std::sync::Weak<CommitRuntimeInner>>>,
}

impl RuntimePoisonCell {
    pub(super) fn bind(&self, runtime: &Arc<CommitRuntimeInner>) {
        *self.runtime.lock() = Some(Arc::downgrade(runtime));
    }

    pub(super) fn poison(&self, poison: CommitRuntimePoison) {
        let Some(runtime) = self.runtime.lock().as_ref().and_then(|weak| weak.upgrade()) else {
            return;
        };
        runtime.poison(poison);
    }
}
