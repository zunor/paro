// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Execution-receipt terminal transitions for a result handler.

use super::ResultHandler;

impl ResultHandler {
    pub(super) fn mark_closed(&mut self) {
        if let Some(receipt) = self.execution_receipt.take() {
            receipt.complete();
        }
        self.closed = true;
        self.output = super::ResultOutput::Closed;
        self.detach_query_memory_pool();
    }

    pub(super) fn mark_cancelled(&mut self, error: impl Into<String>) {
        if let Some(receipt) = self.execution_receipt.take() {
            receipt.cancel(error);
        }
        self.closed = true;
        self.output = super::ResultOutput::Closed;
        self.detach_query_memory_pool();
    }

    pub(super) fn mark_failed(&mut self, error: impl Into<String>) {
        if let Some(receipt) = self.execution_receipt.take() {
            receipt.fail(error);
        }
        self.closed = true;
        self.output = super::ResultOutput::Closed;
        self.detach_query_memory_pool();
    }
}
