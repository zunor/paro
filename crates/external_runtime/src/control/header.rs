// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CONTROL_HEADER_VERSION: u16 = 1;
pub const CONTROL_HEADER_SIZE: usize = 32;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlMessageKind {
    Submit = 1,
    Cancel = 2,
    Complete = 3,
    CreditReturn = 4,
    Error = 5,
}

impl ControlMessageKind {
    pub fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(Self::Submit),
            2 => Some(Self::Cancel),
            3 => Some(Self::Complete),
            4 => Some(Self::CreditReturn),
            5 => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ControlHeaderError {
    #[error("control header requires exactly {CONTROL_HEADER_SIZE} bytes")]
    InvalidSize,
    #[error("unsupported control header version {0}")]
    UnsupportedVersion(u16),
    #[error("unknown control message kind {0}")]
    UnknownKind(u16),
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ControlHeader {
    pub version: u16,
    pub kind: u16,
    pub flags: u32,
    pub batch_id: u64,
    pub lease_id: u64,
    pub payload_len: u32,
    pub reserved: u32,
}

impl ControlHeader {
    pub fn new(kind: ControlMessageKind, batch_id: u64, lease_id: u64, payload_len: u32) -> Self {
        Self {
            version: CONTROL_HEADER_VERSION,
            kind: kind as u16,
            flags: 0,
            batch_id,
            lease_id,
            payload_len,
            reserved: 0,
        }
    }

    pub fn kind(&self) -> Result<ControlMessageKind, ControlHeaderError> {
        ControlMessageKind::from_raw(self.kind).ok_or(ControlHeaderError::UnknownKind(self.kind))
    }

    pub fn encode(&self) -> [u8; CONTROL_HEADER_SIZE] {
        let mut bytes = [0_u8; CONTROL_HEADER_SIZE];
        bytes[0..2].copy_from_slice(&self.version.to_le_bytes());
        bytes[2..4].copy_from_slice(&self.kind.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.flags.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.batch_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.lease_id.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.reserved.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ControlHeaderError> {
        let bytes: [u8; CONTROL_HEADER_SIZE] = bytes
            .try_into()
            .map_err(|_| ControlHeaderError::InvalidSize)?;

        let header = Self {
            version: u16::from_le_bytes(bytes[0..2].try_into().expect("version bytes")),
            kind: u16::from_le_bytes(bytes[2..4].try_into().expect("kind bytes")),
            flags: u32::from_le_bytes(bytes[4..8].try_into().expect("flags bytes")),
            batch_id: u64::from_le_bytes(bytes[8..16].try_into().expect("batch bytes")),
            lease_id: u64::from_le_bytes(bytes[16..24].try_into().expect("lease bytes")),
            payload_len: u32::from_le_bytes(bytes[24..28].try_into().expect("payload bytes")),
            reserved: u32::from_le_bytes(bytes[28..32].try_into().expect("reserved bytes")),
        };

        if header.version != CONTROL_HEADER_VERSION {
            return Err(ControlHeaderError::UnsupportedVersion(header.version));
        }
        header.kind()?;
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlHeader, ControlMessageKind, CONTROL_HEADER_SIZE};

    #[test]
    fn control_header_stays_fixed_layout() {
        assert_eq!(std::mem::size_of::<ControlHeader>(), CONTROL_HEADER_SIZE);
        assert_eq!(std::mem::align_of::<ControlHeader>(), 8);
    }

    #[test]
    fn control_header_roundtrips_bytes() {
        let header = ControlHeader::new(ControlMessageKind::Submit, 9, 12, 64);
        let encoded = header.encode();
        let decoded = ControlHeader::decode(&encoded).expect("decode header");
        assert_eq!(decoded, header);
        assert_eq!(decoded.kind().expect("kind"), ControlMessageKind::Submit);
    }
}
