// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use crate::runtime::{CleanupReason, RUNTIME_WAIT_TIMEOUT};

use super::{ResultHandler, ResultOutput};

impl ResultHandler {
    #[inline]
    pub(super) fn fetch_background_output(&mut self) -> Result<Option<&Chunk>> {
        loop {
            if self.cancellation.is_cancelled() {
                let _ = self.cleanup_typed_driver(CleanupReason::Cancelled(
                    self.cancellation
                        .reason()
                        .unwrap_or(paro_context::StatementCancelReason::UserRequest),
                ));
                self.mark_closed();
                self.cancellation.check()?;
                return Ok(None);
            }

            if let Some(chunk) = self.pop_background_output_chunk()? {
                self.output_chunk = chunk;
                if self.output_chunk.size() != 0 {
                    return Ok(Some(&self.output_chunk));
                }
                continue;
            }

            if self.background_driver_finished()? {
                let result = self.finish_background_driver();
                self.mark_closed();
                result?;
                return Ok(None);
            }

            self.wait_for_background_progress()?;
        }
    }

    #[inline]
    pub(super) fn pop_background_output_chunk(&self) -> Result<Option<Chunk>> {
        let ResultOutput::Background { output, .. } = &self.output else {
            return Err(paro_common::error::internal(
                "background output path selected without output port",
            ));
        };
        Ok(output.pop_front())
    }

    #[inline]
    pub(super) fn background_driver_finished(&self) -> Result<bool> {
        let ResultOutput::Background { driver, .. } = &self.output else {
            return Err(paro_common::error::internal(
                "background output path selected without driver",
            ));
        };
        Ok(driver.is_finished())
    }

    fn wait_for_background_progress(&self) -> Result<()> {
        let ResultOutput::Background { output, .. } = &self.output else {
            return Err(paro_common::error::internal(
                "background output path selected without output port",
            ));
        };
        output.wait_for_change_timeout(RUNTIME_WAIT_TIMEOUT);
        Ok(())
    }

    pub(super) fn finish_background_driver(&mut self) -> Result<()> {
        let output = std::mem::replace(&mut self.output, ResultOutput::Closed);
        match output {
            ResultOutput::Background { mut driver, .. } => driver.join(),
            other => {
                self.output = other;
                Ok(())
            }
        }
    }
}
