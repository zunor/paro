// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Process-level system reserve budget.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

use paro_common::memory::{MemoryDomain, MemoryError, MemoryResult};
use paro_storage::buffer::WriteBufferReserve;

use super::MemoryArbitrator;

const SYSTEM_RESERVE_CLASS_COUNT: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum SystemReserveClass {
    WriteBuffer = 0,
    Maintenance = 1,
    Spill = 2,
    SessionRetained = 3,
}

impl SystemReserveClass {
    fn index(self) -> usize {
        self as usize
    }
}

/// Capacity tracked outside one query but inside the process memory envelope.
pub struct SystemReserve {
    arbitrator: Arc<MemoryArbitrator>,
    used: [AtomicUsize; SYSTEM_RESERVE_CLASS_COUNT],
    limits: RwLock<[usize; SYSTEM_RESERVE_CLASS_COUNT]>,
}

impl SystemReserve {
    pub fn new(arbitrator: Arc<MemoryArbitrator>) -> Self {
        Self {
            arbitrator,
            used: std::array::from_fn(|_| AtomicUsize::new(0)),
            limits: RwLock::new([usize::MAX; SYSTEM_RESERVE_CLASS_COUNT]),
        }
    }

    pub fn set_class_limit(&self, class: SystemReserveClass, limit: usize) {
        self.limits
            .write()
            .expect("system reserve limits lock poisoned")[class.index()] = limit;
    }

    pub fn used_bytes(&self, class: SystemReserveClass) -> usize {
        self.used[class.index()].load(Ordering::Acquire)
    }

    pub fn total_used_bytes(&self) -> usize {
        self.used.iter().fold(0usize, |sum, used| {
            sum.saturating_add(used.load(Ordering::Acquire))
        })
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        class: SystemReserveClass,
        bytes: usize,
    ) -> MemoryResult<SystemReserveReservation> {
        self.try_acquire_bytes(class, bytes)?;
        Ok(SystemReserveReservation::new(
            Arc::downgrade(self),
            class,
            bytes,
        ))
    }

    pub fn try_acquire_bytes(&self, class: SystemReserveClass, bytes: usize) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }

        let idx = class.index();
        let limit = self
            .limits
            .read()
            .expect("system reserve limits lock poisoned")[idx];
        let mut current = self.used[idx].load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(MemoryError::quota_exhausted(
                    MemoryDomain::Host,
                    bytes,
                    limit.saturating_sub(current),
                ));
            };
            if next > limit {
                return Err(MemoryError::quota_exhausted(
                    MemoryDomain::Host,
                    bytes,
                    limit.saturating_sub(current),
                ));
            }
            match self.used[idx].compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.arbitrator.add_system_reserve_bytes(bytes);
                    return Ok(());
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, class: SystemReserveClass, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let idx = class.index();
        let _ = self.used[idx].fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(bytes))
        });
        self.arbitrator.release_system_reserve_bytes(bytes);
    }
}

impl fmt::Debug for SystemReserve {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemReserve")
            .field(
                "write_buffer_bytes",
                &self.used_bytes(SystemReserveClass::WriteBuffer),
            )
            .field(
                "maintenance_bytes",
                &self.used_bytes(SystemReserveClass::Maintenance),
            )
            .field("spill_bytes", &self.used_bytes(SystemReserveClass::Spill))
            .field(
                "session_retained_bytes",
                &self.used_bytes(SystemReserveClass::SessionRetained),
            )
            .finish()
    }
}

impl WriteBufferReserve for SystemReserve {
    fn try_acquire(&self, bytes: usize) -> bool {
        self.try_acquire_bytes(SystemReserveClass::WriteBuffer, bytes)
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        SystemReserve::release(self, SystemReserveClass::WriteBuffer, bytes);
    }

    fn reserved_bytes(&self) -> usize {
        self.used_bytes(SystemReserveClass::WriteBuffer)
    }
}

pub struct SystemReserveReservation {
    reserve: Weak<SystemReserve>,
    class: SystemReserveClass,
    bytes: usize,
    released: AtomicBool,
}

impl SystemReserveReservation {
    fn new(reserve: Weak<SystemReserve>, class: SystemReserveClass, bytes: usize) -> Self {
        Self {
            reserve,
            class,
            bytes,
            released: AtomicBool::new(false),
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if let Some(reserve) = self.reserve.upgrade() {
                reserve.release(self.class, self.bytes);
            }
        }
    }
}

impl fmt::Debug for SystemReserveReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemReserveReservation")
            .field("class", &self.class)
            .field("bytes", &self.bytes)
            .field("released", &self.released.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for SystemReserveReservation {
    fn drop(&mut self) {
        self.release();
    }
}
