// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::memory::{MemoryGrant, MemoryResult};

/// Grant-accounted UTF-8 string.
#[derive(Debug)]
pub struct AccountedString {
    inner: String,
    grant: MemoryGrant,
    accounted_bytes: usize,
}

impl AccountedString {
    pub fn new(grant: MemoryGrant) -> Self {
        Self {
            inner: String::new(),
            grant,
            accounted_bytes: 0,
        }
    }

    pub fn try_push_str(&mut self, value: &str) -> MemoryResult<()> {
        let required = self.inner.len().saturating_add(value.len());
        if required > self.inner.capacity() {
            let old = self.accounted_bytes;
            let new = required;
            let delta = new.saturating_sub(old);
            self.grant.try_consume(delta)?;
            match self
                .inner
                .try_reserve_exact(required.saturating_sub(self.inner.capacity()))
            {
                Ok(()) => {
                    let actual = self.inner.capacity();
                    if actual > new {
                        self.grant.try_consume(actual - new)?;
                    } else if new > actual {
                        self.grant.refund(new - actual);
                    }
                    self.accounted_bytes = actual;
                }
                Err(_) => {
                    self.grant.refund(delta);
                    return Err(crate::memory::MemoryError::physical_allocation_failed(
                        delta,
                    ));
                }
            }
        }
        self.inner.push_str(value);
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }
}

impl Drop for AccountedString {
    fn drop(&mut self) {
        self.grant.refund(self.accounted_bytes);
        self.accounted_bytes = 0;
    }
}
