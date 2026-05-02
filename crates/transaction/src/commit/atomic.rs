// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Atomic helpers shared by commit hot-path modules.

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn fetch_max_relaxed(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}
