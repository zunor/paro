// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Mutex;

use paro_common::error::{self as paro_error, Result};
use paro_context::StatementCancelReason;

use crate::pipeline::graph::SharedSinkId;
use crate::runtime::context::QueryErrorId;

const PRODUCER_COUNT_NOT_FROZEN: usize = usize::MAX;

#[derive(Debug)]
pub struct SharedSinkCoordinator {
    id: SharedSinkId,
    producer_count: AtomicUsize,
    frozen_producer_count: AtomicUsize,
    merged_count: AtomicUsize,
    state: AtomicU8,
    finish_claimed: AtomicBool,
    registration: Mutex<ProducerRegistration>,
    terminal: Mutex<Option<SharedSinkTerminalReason>>,
}

impl SharedSinkCoordinator {
    pub fn new(id: SharedSinkId) -> Self {
        Self {
            id,
            producer_count: AtomicUsize::new(0),
            frozen_producer_count: AtomicUsize::new(PRODUCER_COUNT_NOT_FROZEN),
            merged_count: AtomicUsize::new(0),
            state: AtomicU8::new(SharedSinkStateCode::Open as u8),
            finish_claimed: AtomicBool::new(false),
            registration: Mutex::new(ProducerRegistration::default()),
            terminal: Mutex::new(None),
        }
    }

    #[inline]
    pub fn id(&self) -> SharedSinkId {
        self.id
    }

    pub fn register_producer(&self) -> Result<SharedSinkProducerIndex> {
        self.ensure_open()?;
        let mut registration = self
            .registration
            .lock()
            .expect("shared sink registration poisoned");
        if registration.frozen {
            return Err(paro_error::internal(
                "shared sink producer registration is frozen",
            ));
        }
        let index = registration.count;
        registration.count += 1;
        self.producer_count
            .store(registration.count, Ordering::Release);
        Ok(SharedSinkProducerIndex(index))
    }

    pub fn producer_count(&self) -> usize {
        self.producer_count.load(Ordering::Acquire)
    }

    pub fn freeze_producer_count(&self) -> Result<usize> {
        self.ensure_open()?;
        let mut registration = self
            .registration
            .lock()
            .expect("shared sink registration poisoned");
        registration.frozen = true;
        self.frozen_producer_count
            .store(registration.count, Ordering::Release);
        Ok(registration.count)
    }

    pub fn frozen_producer_count(&self) -> Option<usize> {
        match self.frozen_producer_count.load(Ordering::Acquire) {
            PRODUCER_COUNT_NOT_FROZEN => None,
            count => Some(count),
        }
    }

    pub fn merged_count(&self) -> usize {
        self.merged_count.load(Ordering::Acquire)
    }

    pub fn mark_producer_merged(&self) -> Result<SharedSinkMergeEvent> {
        self.ensure_open()?;
        let Some(producers) = self.frozen_producer_count() else {
            return Err(paro_error::internal(
                "shared sink producer count must be frozen before merge",
            ));
        };
        if producers == 0 {
            return Err(paro_error::internal(
                "shared sink producer merged before registration",
            ));
        }

        let prev = self.merged_count.fetch_add(1, Ordering::AcqRel);
        if prev >= producers {
            self.merged_count.fetch_sub(1, Ordering::AcqRel);
            return Err(paro_error::internal("shared sink producer merged twice"));
        }

        if prev + 1 == producers {
            let _ = self.state.compare_exchange(
                SharedSinkStateCode::Open as u8,
                SharedSinkStateCode::Sealing as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            Ok(SharedSinkMergeEvent::ReadyToFinish)
        } else {
            Ok(SharedSinkMergeEvent::WaitingForProducers {
                remaining: producers - (prev + 1),
            })
        }
    }

    pub fn try_begin_finish(&self) -> Result<bool> {
        match self.state_code() {
            SharedSinkStateCode::Sealing => Ok(self
                .finish_claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()),
            SharedSinkStateCode::Finished => Ok(false),
            SharedSinkStateCode::Open => Err(paro_error::internal(
                "shared sink finish requested before merge barrier",
            )),
            SharedSinkStateCode::Failed | SharedSinkStateCode::Cancelled => {
                Err(self.terminal_error())
            }
        }
    }

    pub fn mark_finished(&self) -> Result<()> {
        if !self.finish_claimed.load(Ordering::Acquire) {
            return Err(paro_error::internal(
                "shared sink finished before finish was claimed",
            ));
        }
        match self.state_code() {
            SharedSinkStateCode::Sealing => {
                self.state
                    .store(SharedSinkStateCode::Finished as u8, Ordering::Release);
                Ok(())
            }
            SharedSinkStateCode::Finished => Ok(()),
            SharedSinkStateCode::Failed | SharedSinkStateCode::Cancelled => {
                Err(self.terminal_error())
            }
            SharedSinkStateCode::Open => Err(paro_error::internal(
                "shared sink finished before merge barrier",
            )),
        }
    }

    pub fn fail(&self, error: QueryErrorId) -> bool {
        self.set_terminal(SharedSinkTerminalReason::Failed(error))
    }

    pub fn cancel(&self, reason: StatementCancelReason) -> bool {
        self.set_terminal(SharedSinkTerminalReason::Cancelled(reason))
    }

    pub fn state(&self) -> SharedSinkState {
        match self.state_code() {
            SharedSinkStateCode::Open => SharedSinkState::Open,
            SharedSinkStateCode::Sealing => SharedSinkState::Sealing,
            SharedSinkStateCode::Finished => SharedSinkState::Finished,
            SharedSinkStateCode::Failed | SharedSinkStateCode::Cancelled => {
                match *self.terminal.lock().expect("shared sink terminal poisoned") {
                    Some(SharedSinkTerminalReason::Failed(error)) => SharedSinkState::Failed(error),
                    Some(SharedSinkTerminalReason::Cancelled(reason)) => {
                        SharedSinkState::Cancelled(reason)
                    }
                    None => SharedSinkState::Failed(QueryErrorId::UNKNOWN),
                }
            }
        }
    }

    fn ensure_open(&self) -> Result<()> {
        match self.state_code() {
            SharedSinkStateCode::Open => Ok(()),
            SharedSinkStateCode::Failed | SharedSinkStateCode::Cancelled => {
                Err(self.terminal_error())
            }
            SharedSinkStateCode::Sealing | SharedSinkStateCode::Finished => Err(
                paro_error::internal("shared sink is no longer accepting producer input"),
            ),
        }
    }

    fn set_terminal(&self, terminal: SharedSinkTerminalReason) -> bool {
        let target = match terminal {
            SharedSinkTerminalReason::Failed(_) => SharedSinkStateCode::Failed,
            SharedSinkTerminalReason::Cancelled(_) => SharedSinkStateCode::Cancelled,
        };
        let mut terminal_guard = self.terminal.lock().expect("shared sink terminal poisoned");
        loop {
            match self.state_code() {
                SharedSinkStateCode::Finished
                | SharedSinkStateCode::Failed
                | SharedSinkStateCode::Cancelled => return false,
                current => {
                    if self
                        .state
                        .compare_exchange(
                            current as u8,
                            target as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        *terminal_guard = Some(terminal);
                        return true;
                    }
                }
            }
        }
    }

    fn terminal_error(&self) -> paro_common::error::ParoError {
        match self.state() {
            SharedSinkState::Failed(error) => {
                paro_error::internal(format!("shared sink failed after query error {:?}", error))
            }
            SharedSinkState::Cancelled(reason) => {
                paro_error::internal(format!("shared sink was cancelled: {:?}", reason))
            }
            _ => paro_error::internal("shared sink is not in a terminal error state"),
        }
    }

    fn state_code(&self) -> SharedSinkStateCode {
        SharedSinkStateCode::from_u8(self.state.load(Ordering::Acquire))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedSinkProducerIndex(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedSinkMergeEvent {
    WaitingForProducers { remaining: usize },
    ReadyToFinish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedSinkState {
    Open,
    Sealing,
    Finished,
    Failed(QueryErrorId),
    Cancelled(StatementCancelReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedSinkTerminalReason {
    Failed(QueryErrorId),
    Cancelled(StatementCancelReason),
}

#[derive(Debug, Default)]
struct ProducerRegistration {
    count: usize,
    frozen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedSinkStateCode {
    Open = 0,
    Sealing = 1,
    Finished = 2,
    Failed = 3,
    Cancelled = 4,
}

impl SharedSinkStateCode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Sealing,
            2 => Self::Finished,
            3 => Self::Failed,
            4 => Self::Cancelled,
            _ => Self::Open,
        }
    }
}

#[cfg(test)]
mod tests {
    use paro_context::StatementCancelReason;

    use super::*;

    #[test]
    fn coordinator_releases_finish_once_after_all_producers_merge() {
        let coordinator = SharedSinkCoordinator::new(SharedSinkId::new(0));
        assert_eq!(
            coordinator
                .register_producer()
                .expect("producer 0 should register"),
            SharedSinkProducerIndex(0)
        );
        assert_eq!(
            coordinator
                .register_producer()
                .expect("producer 1 should register"),
            SharedSinkProducerIndex(1)
        );
        assert_eq!(
            coordinator
                .freeze_producer_count()
                .expect("freeze should succeed"),
            2
        );
        assert!(coordinator.register_producer().is_err());

        assert_eq!(
            coordinator
                .mark_producer_merged()
                .expect("first merge should wait"),
            SharedSinkMergeEvent::WaitingForProducers { remaining: 1 }
        );
        assert_eq!(
            coordinator
                .mark_producer_merged()
                .expect("second merge should release finish"),
            SharedSinkMergeEvent::ReadyToFinish
        );

        assert!(coordinator
            .try_begin_finish()
            .expect("first finish claim should work"));
        assert!(!coordinator
            .try_begin_finish()
            .expect("second finish claim should be ignored"));
        coordinator
            .mark_finished()
            .expect("claimed finish can complete");
        assert_eq!(coordinator.state(), SharedSinkState::Finished);
    }

    #[test]
    fn coordinator_records_failure_and_rejects_late_merge() {
        let coordinator = SharedSinkCoordinator::new(SharedSinkId::new(0));
        coordinator.register_producer().expect("producer");
        assert!(coordinator.fail(QueryErrorId::new(7)));
        assert_eq!(
            coordinator.state(),
            SharedSinkState::Failed(QueryErrorId::new(7))
        );
        assert!(coordinator.mark_producer_merged().is_err());
        assert!(!coordinator.cancel(StatementCancelReason::UserRequest));
    }

    #[test]
    fn coordinator_rejects_remaining_producers_after_partial_merge_failure() {
        let coordinator = SharedSinkCoordinator::new(SharedSinkId::new(0));
        coordinator.register_producer().expect("first producer");
        coordinator.register_producer().expect("second producer");
        coordinator.freeze_producer_count().expect("freeze");

        assert_eq!(
            coordinator
                .mark_producer_merged()
                .expect("first producer should wait"),
            SharedSinkMergeEvent::WaitingForProducers { remaining: 1 }
        );
        assert!(coordinator.fail(QueryErrorId::new(11)));

        let merge_err = coordinator
            .mark_producer_merged()
            .expect_err("remaining producer should see failure");
        assert!(merge_err.message().contains("shared sink failed"));

        let finish_err = coordinator
            .try_begin_finish()
            .expect_err("finish owner should see failure");
        assert!(finish_err.message().contains("shared sink failed"));
        assert_eq!(coordinator.merged_count(), 1);
        assert_eq!(
            coordinator.state(),
            SharedSinkState::Failed(QueryErrorId::new(11))
        );
    }

    #[test]
    fn coordinator_requires_frozen_producers_before_merge() {
        let coordinator = SharedSinkCoordinator::new(SharedSinkId::new(0));
        coordinator.register_producer().expect("producer");

        let err = coordinator
            .mark_producer_merged()
            .expect_err("merge before freeze should fail");

        assert!(err.to_string().contains("frozen"));
        assert_eq!(coordinator.merged_count(), 0);
    }

    #[test]
    fn coordinator_rejects_finish_after_cancel() {
        let coordinator = SharedSinkCoordinator::new(SharedSinkId::new(0));
        coordinator.register_producer().expect("producer");
        coordinator.freeze_producer_count().expect("freeze");
        assert!(coordinator.cancel(StatementCancelReason::UserRequest));

        assert!(coordinator.try_begin_finish().is_err());
        assert_eq!(
            coordinator.state(),
            SharedSinkState::Cancelled(StatementCancelReason::UserRequest)
        );
    }

    #[test]
    fn coordinator_rejects_finish_owner_after_cancel_during_sealing() {
        let coordinator = SharedSinkCoordinator::new(SharedSinkId::new(0));
        coordinator.register_producer().expect("producer");
        coordinator.freeze_producer_count().expect("freeze");

        assert_eq!(
            coordinator
                .mark_producer_merged()
                .expect("producer merge should release finish"),
            SharedSinkMergeEvent::ReadyToFinish
        );
        assert!(coordinator
            .try_begin_finish()
            .expect("finish should be claimed"));
        assert!(coordinator.cancel(StatementCancelReason::UserRequest));

        let finish_err = coordinator
            .mark_finished()
            .expect_err("claimed finish owner should see cancellation");
        assert!(finish_err.message().contains("cancelled"));
        assert!(coordinator.try_begin_finish().is_err());
        assert_eq!(
            coordinator.state(),
            SharedSinkState::Cancelled(StatementCancelReason::UserRequest)
        );
    }
}
