//! Decode helpers for storage payloads.

use paro_common::error::{self as paro_error, Result};

pub(crate) fn decode_varlen_cell(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(paro_error::data_corrupted(
            "varlen decode failed: missing length prefix",
        ));
    }
    let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let value_start = 4usize;
    let value_end = value_start
        .checked_add(len)
        .ok_or_else(|| paro_error::data_corrupted("varlen decode overflow"))?;
    if value_end > bytes.len() {
        return Err(paro_error::data_corrupted(
            "varlen decode failed: value length out of bounds",
        ));
    }
    if value_end != bytes.len() {
        return Err(paro_error::data_corrupted(
            "varlen decode failed: trailing bytes detected",
        ));
    }
    Ok(&bytes[value_start..value_end])
}
