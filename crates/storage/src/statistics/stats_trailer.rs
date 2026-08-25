// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stats trailer helpers for persisting optional statistics blobs.

use paro_common::error::{self as paro_error, Result};
use std::io::Write;

pub(crate) const STATS_TRAILER_MAGIC: u32 = u32::from_le_bytes(*b"STAT");

/// Append a stats trailer to the buffer.
///
/// Layout: [stats bytes][len:u32][magic:u32]
pub(crate) fn append_stats_trailer(buf: &mut Vec<u8>, stats_bytes: &[u8]) -> Result<()> {
    write_stats_trailer(buf, stats_bytes)
}

pub(crate) fn write_stats_trailer<W: Write>(mut writer: W, stats_bytes: &[u8]) -> Result<()> {
    if stats_bytes.is_empty() {
        return Ok(());
    }
    let len = u32::try_from(stats_bytes.len())
        .map_err(|_| paro_error::out_of_range("stats trailer too large"))?;
    writer.write_all(stats_bytes)?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&STATS_TRAILER_MAGIC.to_le_bytes())?;
    Ok(())
}

/// Split stats trailer from the data buffer, if present.
///
/// Returns (stats_bytes, data_without_trailer).
pub(crate) fn split_stats_trailer(data: &[u8]) -> (Option<&[u8]>, &[u8]) {
    const TRAILER_LEN: usize = 8;
    if data.len() < TRAILER_LEN {
        return (None, data);
    }

    let magic = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
    if magic != STATS_TRAILER_MAGIC {
        return (None, data);
    }

    let len = u32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap()) as usize;
    if len == 0 || data.len() < TRAILER_LEN + len {
        return (None, data);
    }

    let stats_start = data.len() - TRAILER_LEN - len;
    let stats = &data[stats_start..stats_start + len];
    let without = &data[..stats_start];
    (Some(stats), without)
}
