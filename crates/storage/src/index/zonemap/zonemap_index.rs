//! # ZoneMap Index Implementation
//!
//! Per-page min/max/has_null statistics for predicate pushdown.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};

/// ZoneMap entry for a single page.
#[derive(Debug, Clone)]
pub struct ZoneMapEntry {
    /// Minimum value in the page (serialized bytes)
    pub min: Bytes,
    /// Maximum value in the page (serialized bytes)
    pub max: Bytes,
    /// Whether the page contains null values
    pub has_null: bool,
}

impl ZoneMapEntry {
    /// Create a new zone map entry.
    pub fn new(min: Bytes, max: Bytes, has_null: bool) -> Self {
        ZoneMapEntry { min, max, has_null }
    }
}

/// ZoneMap index writer - stores min/max/has_null per page.
#[derive(Debug)]
pub struct ZoneMapIndexWriter {
    /// Per-page zone maps
    entries: Vec<ZoneMapEntry>,
    /// Global min value
    global_min: Option<Bytes>,
    /// Global max value
    global_max: Option<Bytes>,
    /// Global has_null flag
    global_has_null: bool,
}

impl ZoneMapIndexWriter {
    /// Create a new zone map index writer.
    pub fn new() -> Self {
        ZoneMapIndexWriter {
            entries: Vec::new(),
            global_min: None,
            global_max: None,
            global_has_null: false,
        }
    }

    /// Add a zone map entry for a page.
    pub fn add(&mut self, min: Bytes, max: Bytes, has_null: bool) {
        // Update global stats using byte comparison
        if self.global_min.is_none() || min < *self.global_min.as_ref().unwrap() {
            self.global_min = Some(min.clone());
        }
        if self.global_max.is_none() || max > *self.global_max.as_ref().unwrap() {
            self.global_max = Some(max.clone());
        }
        if has_null {
            self.global_has_null = true;
        }

        self.entries.push(ZoneMapEntry { min, max, has_null });
    }

    /// Add a zone map entry with custom comparator.
    pub fn add_with_cmp<F>(&mut self, min: Bytes, max: Bytes, has_null: bool, cmp: F)
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        // Update global stats using custom comparator
        if self.global_min.is_none()
            || cmp(&min, self.global_min.as_ref().unwrap()) == std::cmp::Ordering::Less
        {
            self.global_min = Some(min.clone());
        }
        if self.global_max.is_none()
            || cmp(&max, self.global_max.as_ref().unwrap()) == std::cmp::Ordering::Greater
        {
            self.global_max = Some(max.clone());
        }
        if has_null {
            self.global_has_null = true;
        }

        self.entries.push(ZoneMapEntry { min, max, has_null });
    }

    /// Finish and serialize the index.
    ///
    /// Format:
    /// ```text
    /// global_min_len(4) | global_min | global_max_len(4) | global_max | global_has_null(1)
    /// num_entries(4)
    /// [min_len(4) | min | max_len(4) | max | has_null(1)] * num_entries
    /// ```
    pub fn finish(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Write global zone map
        Self::write_value(&mut buf, self.global_min.as_ref());
        Self::write_value(&mut buf, self.global_max.as_ref());
        buf.put_u8(if self.global_has_null { 1 } else { 0 });

        // Write per-page zone maps
        buf.put_u32_le(self.entries.len() as u32);
        for entry in &self.entries {
            Self::write_value(&mut buf, Some(&entry.min));
            Self::write_value(&mut buf, Some(&entry.max));
            buf.put_u8(if entry.has_null { 1 } else { 0 });
        }

        buf.freeze()
    }

    fn write_value(buf: &mut BytesMut, value: Option<&Bytes>) {
        match value {
            Some(v) => {
                buf.put_u32_le(v.len() as u32);
                buf.extend_from_slice(v);
            }
            None => {
                buf.put_u32_le(0);
            }
        }
    }

    /// Get global min value.
    pub fn global_min(&self) -> Option<&Bytes> {
        self.global_min.as_ref()
    }

    /// Get global max value.
    pub fn global_max(&self) -> Option<&Bytes> {
        self.global_max.as_ref()
    }

    /// Check if any page has null values.
    pub fn has_null(&self) -> bool {
        self.global_has_null
    }

    /// Get the number of entries.
    pub fn num_entries(&self) -> usize {
        self.entries.len()
    }
}

impl Default for ZoneMapIndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// ZoneMap index reader - reads min/max/has_null per page.
#[derive(Debug, Clone)]
pub struct ZoneMapIndexReader {
    /// Global min value
    pub global_min: Option<Bytes>,
    /// Global max value
    pub global_max: Option<Bytes>,
    /// Global has_null flag
    pub global_has_null: bool,
    /// Per-page zone maps
    entries: Vec<ZoneMapEntry>,
}

impl ZoneMapIndexReader {
    /// Create from serialized index data.
    pub fn from_bytes(data: &Bytes) -> Result<Self> {
        let mut buf = data.as_ref();

        // Read global zone map
        let global_min = Self::read_value(&mut buf)?;
        let global_max = Self::read_value(&mut buf)?;

        if buf.remaining() < 1 {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: missing global has_null",
            ));
        }
        let global_has_null = buf.get_u8() != 0;

        // Read per-page zone maps
        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: missing entry count",
            ));
        }
        let num_entries = buf.get_u32_le() as usize;

        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let min = Self::read_value(&mut buf)?
                .ok_or_else(|| paro_error::data_corrupted("ZoneMapIndexReader: missing min"))?;
            let max = Self::read_value(&mut buf)?
                .ok_or_else(|| paro_error::data_corrupted("ZoneMapIndexReader: missing max"))?;

            if buf.remaining() < 1 {
                return Err(paro_error::data_corrupted(
                    "ZoneMapIndexReader: missing has_null",
                ));
            }
            let has_null = buf.get_u8() != 0;

            entries.push(ZoneMapEntry { min, max, has_null });
        }

        Ok(ZoneMapIndexReader {
            global_min,
            global_max,
            global_has_null,
            entries,
        })
    }

    fn read_value(buf: &mut &[u8]) -> Result<Option<Bytes>> {
        if buf.remaining() < 4 {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: missing value length",
            ));
        }
        let len = buf.get_u32_le() as usize;
        if len == 0 {
            return Ok(None);
        }
        if buf.remaining() < len {
            return Err(paro_error::data_corrupted(
                "ZoneMapIndexReader: truncated value",
            ));
        }
        let value = Bytes::copy_from_slice(&buf[..len]);
        buf.advance(len);
        Ok(Some(value))
    }

    /// Get the number of pages.
    pub fn num_pages(&self) -> usize {
        self.entries.len()
    }

    /// Get zone map for a page.
    pub fn get_page(&self, idx: usize) -> Option<&ZoneMapEntry> {
        self.entries.get(idx)
    }

    /// Get all entries.
    pub fn entries(&self) -> &[ZoneMapEntry] {
        &self.entries
    }

    /// Check if a page might contain values in the given range.
    ///
    /// Returns true if the page might contain matching values,
    /// false if it can be safely skipped.
    pub fn page_may_contain_range<F>(&self, page_idx: usize, min: &[u8], max: &[u8], cmp: F) -> bool
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        if let Some(entry) = self.entries.get(page_idx) {
            // Page can be skipped if:
            // - page.max < query.min (all values too small)
            // - page.min > query.max (all values too large)
            let page_max_lt_min = cmp(&entry.max, min) == std::cmp::Ordering::Less;
            let page_min_gt_max = cmp(&entry.min, max) == std::cmp::Ordering::Greater;

            !page_max_lt_min && !page_min_gt_max
        } else {
            true // Unknown page, don't skip
        }
    }

    /// Check if a page might contain a specific value.
    pub fn page_may_contain_value<F>(&self, page_idx: usize, value: &[u8], cmp: F) -> bool
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        self.page_may_contain_range(page_idx, value, value, cmp)
    }

    /// Check if a page has null values.
    pub fn page_has_null(&self, page_idx: usize) -> bool {
        self.entries.get(page_idx).is_some_and(|e| e.has_null)
    }

    /// Get the min and max byte slices for a page.
    ///
    /// Returns `Some((min, max))` if the page exists, `None` otherwise.
    pub fn page_min_max(&self, page_idx: usize) -> Option<(&[u8], &[u8])> {
        self.entries
            .get(page_idx)
            .map(|e| (e.min.as_ref(), e.max.as_ref()))
    }

    /// Check if the segment might contain values in the given range.
    pub fn segment_may_contain_range<F>(&self, min: &[u8], max: &[u8], cmp: F) -> bool
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        match (&self.global_min, &self.global_max) {
            (Some(seg_min), Some(seg_max)) => {
                let seg_max_lt_min = cmp(seg_max, min) == std::cmp::Ordering::Less;
                let seg_min_gt_max = cmp(seg_min, max) == std::cmp::Ordering::Greater;
                !seg_max_lt_min && !seg_min_gt_max
            }
            _ => true, // No global stats, don't skip
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn i32_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let va = i32::from_le_bytes([a[0], a[1], a[2], a[3]]);
        let vb = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        va.cmp(&vb)
    }

    #[test]
    fn test_zonemap_roundtrip() {
        let mut writer = ZoneMapIndexWriter::new();

        writer.add(
            Bytes::from_static(&[10, 0, 0, 0]),
            Bytes::from_static(&[20, 0, 0, 0]),
            false,
        );
        writer.add(
            Bytes::from_static(&[30, 0, 0, 0]),
            Bytes::from_static(&[40, 0, 0, 0]),
            true,
        );

        let data = writer.finish();
        let reader = ZoneMapIndexReader::from_bytes(&data).unwrap();

        assert_eq!(reader.num_pages(), 2);
        assert!(reader.global_has_null);

        // Check page 0
        let entry0 = reader.get_page(0).unwrap();
        assert_eq!(entry0.min.as_ref(), &[10, 0, 0, 0]);
        assert_eq!(entry0.max.as_ref(), &[20, 0, 0, 0]);
        assert!(!entry0.has_null);

        // Check page 1
        let entry1 = reader.get_page(1).unwrap();
        assert_eq!(entry1.min.as_ref(), &[30, 0, 0, 0]);
        assert_eq!(entry1.max.as_ref(), &[40, 0, 0, 0]);
        assert!(entry1.has_null);
    }

    #[test]
    fn test_zonemap_filtering() {
        let mut writer = ZoneMapIndexWriter::new();

        writer.add(
            Bytes::from_static(&[10, 0, 0, 0]),
            Bytes::from_static(&[20, 0, 0, 0]),
            false,
        );
        writer.add(
            Bytes::from_static(&[30, 0, 0, 0]),
            Bytes::from_static(&[40, 0, 0, 0]),
            true,
        );

        let data = writer.finish();
        let reader = ZoneMapIndexReader::from_bytes(&data).unwrap();

        // Page 0: [10, 20]
        // Query: 15 - should match
        assert!(reader.page_may_contain_value(0, &[15, 0, 0, 0], i32_cmp));
        // Query: 5 - should not match (too small)
        assert!(!reader.page_may_contain_value(0, &[5, 0, 0, 0], i32_cmp));
        // Query: 25 - should not match (too large)
        assert!(!reader.page_may_contain_value(0, &[25, 0, 0, 0], i32_cmp));

        // Page 1: [30, 40]
        // Query: 35 - should match
        assert!(reader.page_may_contain_value(1, &[35, 0, 0, 0], i32_cmp));
        // Query: 25 - should not match
        assert!(!reader.page_may_contain_value(1, &[25, 0, 0, 0], i32_cmp));

        // Check has_null
        assert!(!reader.page_has_null(0));
        assert!(reader.page_has_null(1));
    }

    #[test]
    fn test_zonemap_range_query() {
        let mut writer = ZoneMapIndexWriter::new();

        writer.add(
            Bytes::from_static(&[10, 0, 0, 0]),
            Bytes::from_static(&[20, 0, 0, 0]),
            false,
        );

        let data = writer.finish();
        let reader = ZoneMapIndexReader::from_bytes(&data).unwrap();

        // Range [15, 25] overlaps with [10, 20]
        assert!(reader.page_may_contain_range(0, &[15, 0, 0, 0], &[25, 0, 0, 0], i32_cmp));

        // Range [5, 9] doesn't overlap with [10, 20]
        assert!(!reader.page_may_contain_range(0, &[5, 0, 0, 0], &[9, 0, 0, 0], i32_cmp));

        // Range [21, 30] doesn't overlap with [10, 20]
        assert!(!reader.page_may_contain_range(0, &[21, 0, 0, 0], &[30, 0, 0, 0], i32_cmp));
    }
}
