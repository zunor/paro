// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

pub type Result<T> = std::result::Result<T, RegistryError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    InvalidShard,
    NoSlotAvailable,
    StaleHandle,
    ReleasedHandle,
    InvalidState,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShard => f.write_str("registry shard is out of range"),
            Self::NoSlotAvailable => f.write_str("registry has no free slot"),
            Self::StaleHandle => f.write_str("registry handle refers to a stale slot generation"),
            Self::ReleasedHandle => f.write_str("registry handle has already been released"),
            Self::InvalidState => f.write_str("registry slot is not in a valid state"),
        }
    }
}

impl std::error::Error for RegistryError {}
