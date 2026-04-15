use crate::primary_key::RowID;
use paro_common::error::{self as paro_error, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const PARTIAL_ROW_MAGIC: u32 = 0x5052_4249; // PRBI
const PARTIAL_ROW_VERSION: u32 = 1;

fn partial_row_path(dir: &Path, segment_id: u32) -> PathBuf {
    dir.join(format!("{}.base_rowids", segment_id))
}

pub fn save_base_rowids(dir: &Path, segment_id: u32, rowids: &[RowID]) -> Result<PathBuf> {
    let path = partial_row_path(dir, segment_id);
    let mut file = File::create(&path).map_err(|e| {
        paro_error::io_error(format!("failed to create partial row sidecar: {}", e))
    })?;
    file.write_all(&PARTIAL_ROW_MAGIC.to_le_bytes())
        .map_err(|e| paro_error::io_error(format!("failed to write partial row magic: {}", e)))?;
    file.write_all(&PARTIAL_ROW_VERSION.to_le_bytes())
        .map_err(|e| paro_error::io_error(format!("failed to write partial row version: {}", e)))?;
    file.write_all(&(rowids.len() as u64).to_le_bytes())
        .map_err(|e| paro_error::io_error(format!("failed to write partial row count: {}", e)))?;
    for rowid in rowids {
        file.write_all(&rowid.to_raw().to_le_bytes()).map_err(|e| {
            paro_error::io_error(format!("failed to write partial row entry: {}", e))
        })?;
    }
    file.flush()
        .map_err(|e| paro_error::io_error(format!("failed to flush partial row sidecar: {}", e)))?;
    Ok(path)
}

pub fn load_base_rowids(dir: &Path, segment_id: u32) -> Result<Option<Vec<RowID>>> {
    let path = partial_row_path(dir, segment_id);
    if !path.exists() {
        return Ok(None);
    }

    let mut file = File::open(&path)
        .map_err(|e| paro_error::io_error(format!("failed to open partial row sidecar: {}", e)))?;
    let mut header = [0u8; 16];
    file.read_exact(&mut header)
        .map_err(|e| paro_error::io_error(format!("failed to read partial row header: {}", e)))?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let count = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;
    if magic != PARTIAL_ROW_MAGIC {
        return Err(paro_error::not_supported(
            "unsupported partial row sidecar magic; rebuild with current format",
        ));
    }
    if version != PARTIAL_ROW_VERSION {
        return Err(paro_error::not_supported(
            "unsupported partial row sidecar version; rebuild with current format",
        ));
    }

    let mut raw = vec![0u8; count * std::mem::size_of::<u64>()];
    file.read_exact(&mut raw)
        .map_err(|e| paro_error::io_error(format!("failed to read partial row entries: {}", e)))?;
    let mut rowids = Vec::with_capacity(count);
    for chunk in raw.chunks_exact(8) {
        rowids.push(RowID::from_raw(u64::from_le_bytes(
            chunk.try_into().unwrap(),
        )));
    }
    Ok(Some(rowids))
}

pub fn load_base_rowids_for_offsets(
    dir: &Path,
    segment_id: u32,
    row_offsets: &[u32],
) -> Result<Option<Vec<RowID>>> {
    let Some(all) = load_base_rowids(dir, segment_id)? else {
        return Ok(None);
    };
    let mut selected = Vec::with_capacity(row_offsets.len());
    for &row_offset in row_offsets {
        let idx = row_offset as usize;
        let rowid = all.get(idx).copied().ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "partial row sidecar missing row offset {} for segment {}",
                row_offset, segment_id
            ))
        })?;
        selected.push(rowid);
    }
    Ok(Some(selected))
}
