// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_instance::RegistryKey;
use std::sync::atomic::{AtomicI32, Ordering};

static NEXT_BACKEND_PID: AtomicI32 = AtomicI32::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BackendProcessId(i32);

impl BackendProcessId {
    pub fn value(self) -> i32 {
        self.0
    }

    fn generate() -> Self {
        let previous = NEXT_BACKEND_PID
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(next_backend_pid_value(current))
            })
            .expect("atomic update should not fail");
        Self(next_backend_pid_value(previous))
    }
}

fn next_backend_pid_value(current: i32) -> i32 {
    if current == i32::MAX {
        1
    } else {
        current + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BackendCancelSecret(i32);

impl BackendCancelSecret {
    pub fn value(self) -> i32 {
        self.0
    }

    fn generate() -> Self {
        Self(rand::random::<i32>())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BackendCancelKey {
    pid: BackendProcessId,
    secret: BackendCancelSecret,
}

impl BackendCancelKey {
    pub fn new(pid: BackendProcessId, secret: BackendCancelSecret) -> Self {
        Self { pid, secret }
    }

    pub fn generate() -> Self {
        Self::new(
            BackendProcessId::generate(),
            BackendCancelSecret::generate(),
        )
    }

    pub fn pid(self) -> BackendProcessId {
        self.pid
    }

    pub fn secret(self) -> BackendCancelSecret {
        self.secret
    }

    pub fn registry_key(self) -> RegistryKey {
        RegistryKey::new(self.pid.value(), self.secret.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn backend_pid_generation_skips_zero_and_wraps() {
        assert_eq!(next_backend_pid_value(0), 1);
        assert_eq!(next_backend_pid_value(i32::MAX), 1);
    }

    #[test]
    fn generated_backend_pids_skip_zero() {
        let mut seen = HashSet::new();
        for _ in 0..32 {
            let key = BackendCancelKey::generate();
            assert_ne!(key.pid().value(), 0);
            seen.insert(key.pid().value());
        }
        assert!(seen.len() > 1, "pid generator should advance");
    }
}
