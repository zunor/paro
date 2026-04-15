//! RowSourceMask tracks the source of each row during vertical compaction.

use bytes::BufMut;
use paro_common::error::{self as paro_error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// RowSourceMask stores a uint16_t data that represents the source
/// (segment iterator in compaction) of each row and the aggregation state of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSourceMask {
    data: u16,
}

impl RowSourceMask {
    pub const MAX_SOURCES: u16 = 0x7FFF;
    const MASK_NUMBER: u16 = 0x7FFF;
    const MASK_FLAG: u16 = 0x8000;

    pub fn new(source_num: u16, agg_flag: bool) -> Self {
        let mut mask = Self { data: 0 };
        mask.set_source_num(source_num);
        mask.set_agg_flag(agg_flag);
        mask
    }

    pub fn from_u16(data: u16) -> Self {
        Self { data }
    }

    pub fn as_u16(&self) -> u16 {
        self.data
    }

    pub fn source_num(&self) -> u16 {
        self.data & Self::MASK_NUMBER
    }

    pub fn agg_flag(&self) -> bool {
        (self.data & Self::MASK_FLAG) != 0
    }

    pub fn set_source_num(&mut self, source_num: u16) {
        self.data = (self.data & Self::MASK_FLAG) | (source_num & Self::MASK_NUMBER);
    }

    pub fn set_agg_flag(&mut self, agg_flag: bool) {
        if agg_flag {
            self.data |= Self::MASK_FLAG;
        } else {
            self.data &= !Self::MASK_FLAG;
        }
    }
}

/// RowSourceMaskBuffer stores a series of row source masks, with disk overflow support.
pub struct RowSourceMaskBuffer {
    /// In-memory mask storage.
    masks: Vec<u16>,
    /// Path for temporary file if overflowed.
    tmp_path: Option<PathBuf>,
    /// File handle for disk storage.
    file: Option<File>,
    /// Current read index.
    current_index: u64,
    /// Total number of masks.
    total_masks: u64,
    /// Max memory size before flushing to disk (default 128MB).
    max_memory_size: usize,
}

impl RowSourceMaskBuffer {
    pub fn new(tmp_dir: impl AsRef<Path>) -> Self {
        Self {
            masks: Vec::with_capacity(1024),
            tmp_path: Some(
                tmp_dir
                    .as_ref()
                    .join(format!("row_source_mask_{}.tmp", rand::random::<u64>())),
            ),
            file: None,
            current_index: 0,
            total_masks: 0,
            max_memory_size: 128 * 1024 * 1024, // 128MB
        }
    }

    /// Write masks to the buffer.
    pub fn write(&mut self, source_masks: &[RowSourceMask]) -> Result<()> {
        for mask in source_masks {
            self.masks.push(mask.as_u16());
            self.total_masks += 1;

            if self.masks.len() * 2 >= self.max_memory_size {
                self.flush()?;
            }
        }
        Ok(())
    }

    /// Flush in-memory masks to disk.
    pub fn flush(&mut self) -> Result<()> {
        if self.masks.is_empty() {
            return Ok(());
        }

        if self.file.is_none() {
            let path = self
                .tmp_path
                .as_ref()
                .ok_or_else(|| paro_error::internal("No tmp path"))?;
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(|e| paro_error::io_error(format!("create tmp file {:?}: {}", path, e)))?;
            self.file = Some(f);
        }

        let f = self.file.as_mut().unwrap();
        f.seek(SeekFrom::End(0))
            .map_err(|e| paro_error::io_error(e.to_string()))?;

        let mut bytes = Vec::with_capacity(self.masks.len() * 2);
        for m in &self.masks {
            bytes.put_u16_le(*m);
        }
        f.write_all(&bytes)
            .map_err(|e| paro_error::io_error(e.to_string()))?;

        self.masks.clear();
        Ok(())
    }

    /// Prepare for reading.
    pub fn flip_to_read(&mut self) -> Result<()> {
        self.flush()?;
        self.current_index = 0;
        if let Some(f) = &mut self.file {
            f.seek(SeekFrom::Start(0))
                .map_err(|e| paro_error::io_error(e.to_string()))?;
        }
        Ok(())
    }

    /// Check if there are more masks to read.
    pub fn has_remaining(&self) -> bool {
        self.current_index < self.total_masks
    }

    /// Read the next mask.
    pub fn next(&mut self) -> Result<RowSourceMask> {
        if !self.has_remaining() {
            return Err(paro_error::out_of_range("No more masks"));
        }

        let data = if let Some(f) = &mut self.file {
            let mut buf = [0u8; 2];
            f.read_exact(&mut buf)
                .map_err(|e| paro_error::io_error(e.to_string()))?;
            u16::from_le_bytes(buf)
        } else {
            // Memory only case.
            self.masks[self.current_index as usize]
        };

        self.current_index += 1;
        Ok(RowSourceMask::from_u16(data))
    }
}

impl Drop for RowSourceMaskBuffer {
    fn drop(&mut self) {
        if let Some(path) = &self.tmp_path {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}
