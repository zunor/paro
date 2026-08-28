// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered completion for maintenance records that are applied inline.

use paro_common::error::{self as paro_error, Result};
use paro_journal::{ApplyRequest, JournalApplyRuntime, TabletApplyPart, WaitMode};
use std::sync::{Arc, Mutex};

/// Owns the apply-queue ticket for one already-appended maintenance record.
///
/// Inline storage work records its terminal result before synchronous submit.
/// If control exits early, `Drop` records a fatal missing-result error and
/// submits asynchronously. Consequently every durable LSN reaches the ordered
/// apply runtime exactly once; failures poison the runtime explicitly instead
/// of leaving later records blocked behind an invisible gap.
pub(crate) struct DurableMaintenanceApplyCompletion {
    runtime: Arc<JournalApplyRuntime>,
    request: Option<ApplyRequest<()>>,
    terminal_result: Arc<Mutex<Option<Result<()>>>>,
}

impl DurableMaintenanceApplyCompletion {
    pub(crate) fn arm(
        runtime: Arc<JournalApplyRuntime>,
        lsn: u64,
        durable_batch_lsn: u64,
        tablet_id: u64,
        after_inline_apply: impl FnOnce() -> Result<()> + Send + 'static,
    ) -> Self {
        let terminal_result = Arc::new(Mutex::new(None));
        let apply_terminal = Arc::clone(&terminal_result);
        let request = ApplyRequest {
            lsn,
            durable_batch_lsn,
            commit_id: None,
            wait_mode: WaitMode::Published,
            catalog_serial: false,
            catalog_pre: Box::new(|| Ok(())),
            tablet_parts: vec![TabletApplyPart {
                tablet_id,
                apply: Box::new(move || {
                    let result = apply_terminal
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone()
                        .ok_or_else(|| {
                            paro_error::internal(
                                "durable maintenance apply reached dispatch without a terminal inline result",
                            )
                        })?;
                    result?;
                    after_inline_apply()
                }),
            }],
            descriptor_phase: Box::new(|| Ok(())),
            catalog_post: Box::new(|| Ok(())),
            on_published: Box::new(|| Ok(())),
        };
        Self {
            runtime,
            request: Some(request),
            terminal_result,
        }
    }

    pub(crate) fn record_terminal_result(&self, result: Result<()>) -> Result<()> {
        let mut terminal = self
            .terminal_result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if terminal.is_some() {
            return Err(paro_error::internal(
                "durable maintenance terminal result recorded more than once",
            ));
        }
        *terminal = Some(result);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        let request = self.request.take().ok_or_else(|| {
            paro_error::internal("durable maintenance apply request already consumed")
        })?;
        self.runtime.submit(request)
    }
}

impl Drop for DurableMaintenanceApplyCompletion {
    fn drop(&mut self) {
        let Some(request) = self.request.take() else {
            return;
        };
        {
            let mut terminal = self
                .terminal_result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if terminal.is_none() {
                *terminal = Some(Err(paro_error::internal(
                    "durable maintenance scope exited before recording its inline apply result",
                )));
            }
        }
        if let Err(error) = self.runtime.submit_async(request) {
            tracing::error!(
                error = %error,
                "durable maintenance record could not enter the apply runtime"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn empty_request(lsn: u64) -> ApplyRequest<()> {
        ApplyRequest {
            lsn,
            durable_batch_lsn: lsn,
            commit_id: None,
            wait_mode: WaitMode::Published,
            catalog_serial: false,
            catalog_pre: Box::new(|| Ok(())),
            tablet_parts: Vec::new(),
            descriptor_phase: Box::new(|| Ok(())),
            catalog_post: Box::new(|| Ok(())),
            on_published: Box::new(|| Ok(())),
        }
    }

    #[test]
    fn dropped_successful_completion_cannot_leave_an_lsn_gap() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let observed = Arc::new(AtomicBool::new(false));
        let completion = DurableMaintenanceApplyCompletion::arm(Arc::clone(&runtime), 1, 1, 7, {
            let observed = Arc::clone(&observed);
            move || {
                observed.store(true, Ordering::Release);
                Ok(())
            }
        });
        completion.record_terminal_result(Ok(())).unwrap();
        drop(completion);

        runtime.submit(empty_request(2)).unwrap();
        assert!(observed.load(Ordering::Acquire));
        assert_eq!(runtime.next_dispatch_lsn(), 3);
    }

    #[test]
    fn dropped_unfinished_completion_poison_is_explicit_not_an_lsn_gap() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let completion =
            DurableMaintenanceApplyCompletion::arm(Arc::clone(&runtime), 1, 1, 7, || Ok(()));
        drop(completion);

        let error = runtime.submit(empty_request(2)).unwrap_err();
        let message = error.to_string();
        assert_eq!(message, "journal apply failed after durable append");
        // LSN 2 may register before the asynchronously submitted poison from
        // LSN 1 becomes visible, or may be rejected at registration after the
        // poison wins. The durable invariant is explicit terminal failure,
        // not a particular poisoned frontier value.
        assert!((2..=3).contains(&runtime.next_dispatch_lsn()));
        assert_eq!(
            runtime.submit(empty_request(3)).unwrap_err().to_string(),
            "journal apply failed after durable append"
        );
    }
}
