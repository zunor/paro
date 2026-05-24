// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};

use crate::query_executor::cleanup::{cancelled_cleanup_reason, cleanup_reason_for_error};
use crate::query_executor::pipeline_driver::PipelineDriveResult;
use crate::runtime::CleanupReason;

use super::{ResultHandler, ResultOutput};

impl ResultHandler {
    #[inline]
    pub(super) fn fetch_typed_streaming_output(&mut self) -> Result<Option<&Chunk>> {
        loop {
            if self.cancellation.is_cancelled() {
                self.cleanup_typed_driver_for_cancellation();
                self.mark_closed();
                self.cancellation.check()?;
                return Ok(None);
            }

            while let Some(chunk) = self.pop_typed_output_chunk()? {
                self.output_chunk = chunk;
                if self.output_chunk.size() != 0 {
                    return Ok(Some(&self.output_chunk));
                }
            }

            let result = match self.drive_typed_pipeline() {
                Ok(result) => result,
                Err(error) => {
                    self.cleanup_typed_driver_for_error(&error);
                    self.mark_closed();
                    return Err(error);
                }
            };

            match result {
                PipelineDriveResult::ChunkReady => {}
                PipelineDriveResult::Finished => {
                    let cleanup_result = self.cleanup_typed_driver(CleanupReason::Finished);
                    self.mark_closed();
                    cleanup_result?;
                    return Ok(None);
                }
                PipelineDriveResult::Blocked(reason) => {
                    let error = paro_common::error::internal(format!(
                        "typed streaming execution blocked without client output: {:?}",
                        reason
                    ));
                    self.cleanup_typed_driver_for_error(&error);
                    self.mark_closed();
                    return Err(error);
                }
            }
        }
    }

    #[inline]
    fn pop_typed_output_chunk(&self) -> Result<Option<Chunk>> {
        let ResultOutput::FetchDriven { output, .. } = &self.output else {
            return Err(paro_common::error::internal(
                "typed output path selected without fetch-driven output",
            ));
        };
        Ok(output.pop_front())
    }

    #[inline]
    fn drive_typed_pipeline(&mut self) -> Result<PipelineDriveResult> {
        let ResultOutput::FetchDriven { query, driver, .. } = &mut self.output else {
            return Err(paro_common::error::internal(
                "typed output path selected without fetch-driven driver",
            ));
        };
        driver.drive_until_output_or_finished(query)
    }

    fn cleanup_typed_driver_for_error(&mut self, error: &ParoError) {
        let reason = match &self.output {
            ResultOutput::FetchDriven { query, .. } => cleanup_reason_for_error(query, error),
            _ => return,
        };
        if let Err(cleanup_error) = self.cleanup_typed_driver(reason) {
            tracing::warn!(
                error = %cleanup_error.message(),
                "typed result handler cleanup failed after execution error"
            );
        }
    }

    fn cleanup_typed_driver_for_cancellation(&mut self) {
        let reason = match &self.output {
            ResultOutput::FetchDriven { query, .. } => cancelled_cleanup_reason(query),
            _ => return,
        };
        if let Err(cleanup_error) = self.cleanup_typed_driver(reason) {
            tracing::warn!(
                error = %cleanup_error.message(),
                "typed result handler cleanup failed after cancellation"
            );
        }
    }
}
