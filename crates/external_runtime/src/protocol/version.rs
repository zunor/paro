// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeProtocolVersion {
    pub worker_protocol_version: u16,
    pub abi_version: u16,
}

impl RuntimeProtocolVersion {
    pub const fn current() -> Self {
        Self {
            worker_protocol_version: 1,
            abi_version: 1,
        }
    }

    pub fn cache_key(self) -> String {
        format!("wp{}-abi{}", self.worker_protocol_version, self.abi_version)
    }
}
