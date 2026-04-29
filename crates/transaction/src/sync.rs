// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime synchronization primitives for transaction hot paths.

#[cfg(feature = "runtime")]
use parking_lot as mutex_impl;
#[cfg(not(feature = "runtime"))]
use std::sync as mutex_impl;

pub struct Mutex<T> {
    inner: mutex_impl::Mutex<T>,
}

#[cfg(feature = "runtime")]
pub type MutexGuard<'a, T> = parking_lot::MutexGuard<'a, T>;
#[cfg(not(feature = "runtime"))]
pub type MutexGuard<'a, T> = std::sync::MutexGuard<'a, T>;

impl<T> Mutex<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: mutex_impl::Mutex::new(value),
        }
    }

    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        lock_inner(&self.inner)
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mutex").finish_non_exhaustive()
    }
}

#[cfg(feature = "runtime")]
#[inline]
fn lock_inner<T>(mutex: &parking_lot::Mutex<T>) -> parking_lot::MutexGuard<'_, T> {
    mutex.lock()
}

#[cfg(not(feature = "runtime"))]
#[inline]
fn lock_inner<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().expect("transaction mutex poisoned")
}
