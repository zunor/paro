// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime synchronization primitives for transaction hot paths.

#[cfg(feature = "runtime")]
use parking_lot as mutex_impl;
#[cfg(not(feature = "runtime"))]
use std::sync as mutex_impl;

#[cfg(feature = "runtime")]
use parking_lot as condvar_impl;
#[cfg(not(feature = "runtime"))]
use std::sync as condvar_impl;

pub struct Mutex<T> {
    inner: mutex_impl::Mutex<T>,
}

pub struct Condvar {
    inner: condvar_impl::Condvar,
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

impl Condvar {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: condvar_impl::Condvar::new(),
        }
    }

    #[inline]
    pub fn notify_all(&self) {
        self.inner.notify_all();
    }

    #[inline]
    #[allow(dead_code)]
    pub fn notify_one(&self) {
        self.inner.notify_one();
    }

    #[inline]
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        wait_inner(&self.inner, guard)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: std::time::Duration,
    ) -> (MutexGuard<'a, T>, bool) {
        wait_timeout_inner(&self.inner, guard, timeout)
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Condvar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
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

#[cfg(feature = "runtime")]
#[inline]
fn wait_inner<'a, T>(
    condvar: &parking_lot::Condvar,
    mut guard: parking_lot::MutexGuard<'a, T>,
) -> parking_lot::MutexGuard<'a, T> {
    condvar.wait(&mut guard);
    guard
}

#[cfg(feature = "runtime")]
#[inline]
fn wait_timeout_inner<'a, T>(
    condvar: &parking_lot::Condvar,
    mut guard: parking_lot::MutexGuard<'a, T>,
    timeout: std::time::Duration,
) -> (parking_lot::MutexGuard<'a, T>, bool) {
    let result = condvar.wait_for(&mut guard, timeout);
    (guard, result.timed_out())
}

#[cfg(not(feature = "runtime"))]
#[inline]
fn wait_inner<'a, T>(
    condvar: &std::sync::Condvar,
    guard: std::sync::MutexGuard<'a, T>,
) -> std::sync::MutexGuard<'a, T> {
    condvar.wait(guard).expect("transaction condvar poisoned")
}

#[cfg(not(feature = "runtime"))]
#[inline]
#[allow(dead_code)]
fn wait_timeout_inner<'a, T>(
    condvar: &std::sync::Condvar,
    guard: std::sync::MutexGuard<'a, T>,
    timeout: std::time::Duration,
) -> (std::sync::MutexGuard<'a, T>, bool) {
    let (guard, result) = condvar
        .wait_timeout(guard, timeout)
        .expect("transaction condvar poisoned");
    (guard, result.timed_out())
}
