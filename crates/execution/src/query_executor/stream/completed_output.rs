// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use super::{ResultHandler, ResultOutput};

impl ResultHandler {
    #[inline]
    pub(super) fn fetch_completed_output(&mut self) -> Result<Option<&Chunk>> {
        if self.cancellation.is_cancelled() {
            self.mark_closed();
            self.cancellation.check()?;
            return Ok(None);
        }

        let ResultOutput::Completed(output) = &self.output else {
            return Err(paro_common::error::internal(
                "completed output path selected without output port",
            ));
        };

        while let Some(chunk) = output.pop_front() {
            self.output_chunk = chunk;
            if self.output_chunk.size() != 0 {
                return Ok(Some(&self.output_chunk));
            }
        }

        self.mark_closed();
        Ok(None)
    }
}
