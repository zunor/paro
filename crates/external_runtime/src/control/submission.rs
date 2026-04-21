// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::control::header::{ControlHeader, ControlMessageKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionTicket {
    pub batch_id: u64,
    pub query_id: u64,
    pub shard_key: String,
    pub lease_id: u64,
}

impl SubmissionTicket {
    pub fn submit_header(&self, payload_len: u32) -> ControlHeader {
        ControlHeader::new(
            ControlMessageKind::Submit,
            self.batch_id,
            self.lease_id,
            payload_len,
        )
    }

    pub fn cancel_header(&self) -> ControlHeader {
        ControlHeader::new(ControlMessageKind::Cancel, self.batch_id, self.lease_id, 0)
    }
}
