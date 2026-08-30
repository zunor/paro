// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Predicate Result
//!
//! Result of index predicate evaluation.

use roaring::RoaringBitmap;

use paro_common::error::{self as paro_error, Result};

/// Continuous row range, end is exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRange {
    /// Inclusive start row id.
    pub start_row: u32,
    /// Exclusive end row id.
    pub end_row: u32,
}

impl PageRange {
    /// Create a new range.
    pub fn new(start_row: u32, end_row: u32) -> Self {
        PageRange { start_row, end_row }
    }

    /// Returns true if the range is empty.
    pub fn is_empty(&self) -> bool {
        self.start_row >= self.end_row
    }

    /// Returns true if the row id is within the range.
    pub fn contains(&self, row_id: u32) -> bool {
        row_id >= self.start_row && row_id < self.end_row
    }
}

/// Result of evaluating a predicate with indexes.
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateResult {
    /// All rows match (no filtering needed).
    AllMatch,
    /// No rows match.
    NoneMatch,
    /// Candidate row ids.
    Bitmap(RoaringBitmap),
    /// Candidate row ranges (page-level filtering).
    PageRanges(Vec<PageRange>),
    /// Index cannot evaluate this predicate.
    Unknown,
}

// =============================================================================
// PredicateResult helpers
// =============================================================================

/// Intersect (AND) two predicate results.
pub fn intersect(a: &PredicateResult, b: &PredicateResult) -> PredicateResult {
    use PredicateResult::*;

    match (a, b) {
        (NoneMatch, _) | (_, NoneMatch) => NoneMatch,
        (AllMatch, other) | (other, AllMatch) => other.clone(),
        // Unknown means no filtering info for AND, keep the other side
        (Unknown, other) | (other, Unknown) => other.clone(),
        (Bitmap(left), Bitmap(right)) => {
            let mut out = left.clone();
            out &= right;
            if out.is_empty() {
                NoneMatch
            } else {
                Bitmap(out)
            }
        }
        (Bitmap(bitmap), PageRanges(ranges)) | (PageRanges(ranges), Bitmap(bitmap)) => {
            let filtered = filter_bitmap_by_ranges(bitmap, ranges);
            if filtered.is_empty() {
                NoneMatch
            } else {
                Bitmap(filtered)
            }
        }
        (PageRanges(left), PageRanges(right)) => {
            let ranges = intersect_ranges(left, right);
            if ranges.is_empty() {
                NoneMatch
            } else {
                PageRanges(ranges)
            }
        }
    }
}

/// Union (OR) two predicate results.
pub fn union(a: &PredicateResult, b: &PredicateResult) -> PredicateResult {
    use PredicateResult::*;

    match (a, b) {
        (AllMatch, _) | (_, AllMatch) => AllMatch,
        (NoneMatch, other) | (other, NoneMatch) => other.clone(),
        // OR with unknown cannot be filtered safely
        (Unknown, _) | (_, Unknown) => Unknown,
        (Bitmap(left), Bitmap(right)) => {
            let mut out = left.clone();
            out |= right;
            if out.is_empty() {
                NoneMatch
            } else {
                Bitmap(out)
            }
        }
        (PageRanges(left), PageRanges(right)) => {
            let ranges = union_ranges(left, right);
            if ranges.is_empty() {
                NoneMatch
            } else {
                PageRanges(ranges)
            }
        }
        (Bitmap(bitmap), PageRanges(ranges)) | (PageRanges(ranges), Bitmap(bitmap)) => {
            let mut all_ranges = ranges.clone();
            let mut bitmap_ranges = bitmap_to_ranges(bitmap);
            all_ranges.append(&mut bitmap_ranges);
            let ranges = normalize_ranges(all_ranges);
            if ranges.is_empty() {
                NoneMatch
            } else {
                PageRanges(ranges)
            }
        }
    }
}

/// Convert a predicate result into row ranges if possible.
///
/// Returns None for AllMatch or Unknown, as a total row count is required
/// to materialize those ranges.
pub fn to_row_ranges(result: &PredicateResult) -> Option<Vec<PageRange>> {
    match result {
        PredicateResult::NoneMatch => Some(Vec::new()),
        PredicateResult::Bitmap(bitmap) => Some(bitmap_to_ranges(bitmap)),
        PredicateResult::PageRanges(ranges) => Some(ranges.clone()),
        PredicateResult::AllMatch | PredicateResult::Unknown => None,
    }
}

/// Encode page ranges into bytes for serialization.
pub fn encode_page_ranges(ranges: &[PageRange]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + ranges.len() * 8);
    buf.extend_from_slice(&(ranges.len() as u32).to_le_bytes());
    for range in ranges {
        buf.extend_from_slice(&range.start_row.to_le_bytes());
        buf.extend_from_slice(&range.end_row.to_le_bytes());
    }
    buf
}

/// Decode page ranges from bytes.
pub fn decode_page_ranges(data: &[u8]) -> Result<Vec<PageRange>> {
    if data.len() < 4 {
        return Err(paro_error::data_corrupted("PageRange: data too small"));
    }
    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut ranges = Vec::with_capacity(count);
    let mut offset = 4;
    for _ in 0..count {
        if offset + 8 > data.len() {
            return Err(paro_error::data_corrupted("PageRange: truncated data"));
        }
        let start = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        let end = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap());
        ranges.push(PageRange::new(start, end));
        offset += 8;
    }
    Ok(ranges)
}

// =============================================================================
// Range Helpers
// =============================================================================

fn normalize_ranges(mut ranges: Vec<PageRange>) -> Vec<PageRange> {
    ranges.retain(|r| !r.is_empty());
    ranges.sort_by_key(|r| r.start_row);

    let mut merged: Vec<PageRange> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut() {
            if range.start_row <= last.end_row {
                last.end_row = last.end_row.max(range.end_row);
                continue;
            }
        }
        merged.push(range);
    }
    merged
}

fn intersect_ranges(left: &[PageRange], right: &[PageRange]) -> Vec<PageRange> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut j = 0;
    let left = normalize_ranges(left.to_vec());
    let right = normalize_ranges(right.to_vec());

    while i < left.len() && j < right.len() {
        let a = left[i];
        let b = right[j];
        let start = a.start_row.max(b.start_row);
        let end = a.end_row.min(b.end_row);
        if start < end {
            out.push(PageRange::new(start, end));
        }

        if a.end_row <= b.end_row {
            i += 1;
        } else {
            j += 1;
        }
    }

    out
}

fn union_ranges(left: &[PageRange], right: &[PageRange]) -> Vec<PageRange> {
    let mut combined = left.to_vec();
    combined.extend_from_slice(right);
    normalize_ranges(combined)
}

fn bitmap_to_ranges(bitmap: &RoaringBitmap) -> Vec<PageRange> {
    let mut ranges = Vec::new();
    let mut iter = bitmap.iter();
    let Some(mut start) = iter.next() else {
        return ranges;
    };
    let mut prev = start;
    for value in iter {
        if value == prev + 1 {
            prev = value;
            continue;
        }
        ranges.push(PageRange::new(start, prev + 1));
        start = value;
        prev = value;
    }
    ranges.push(PageRange::new(start, prev + 1));
    ranges
}

fn filter_bitmap_by_ranges(bitmap: &RoaringBitmap, ranges: &[PageRange]) -> RoaringBitmap {
    let mut out = RoaringBitmap::new();
    if ranges.is_empty() {
        return out;
    }

    let ranges = normalize_ranges(ranges.to_vec());
    let mut range_idx = 0usize;

    for row_id in bitmap.iter() {
        while range_idx < ranges.len() && row_id >= ranges[range_idx].end_row {
            range_idx += 1;
        }
        if range_idx >= ranges.len() {
            break;
        }
        if row_id >= ranges[range_idx].start_row {
            out.insert(row_id);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_to_ranges() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmap.insert(4);
        bitmap.insert(5);
        bitmap.insert(8);

        let ranges = bitmap_to_ranges(&bitmap);
        assert_eq!(
            ranges,
            vec![
                PageRange::new(1, 3),
                PageRange::new(4, 6),
                PageRange::new(8, 9),
            ]
        );
    }

    #[test]
    fn test_intersect_ranges() {
        let left = vec![PageRange::new(0, 5), PageRange::new(10, 15)];
        let right = vec![PageRange::new(3, 12)];
        let out = intersect_ranges(&left, &right);
        assert_eq!(out, vec![PageRange::new(3, 5), PageRange::new(10, 12)]);
    }

    #[test]
    fn test_union_ranges() {
        let left = vec![PageRange::new(0, 5)];
        let right = vec![PageRange::new(3, 8), PageRange::new(10, 12)];
        let out = union_ranges(&left, &right);
        assert_eq!(out, vec![PageRange::new(0, 8), PageRange::new(10, 12)]);
    }

    #[test]
    fn test_intersect_predicate_result() {
        let mut left = RoaringBitmap::new();
        left.insert(1);
        left.insert(2);
        left.insert(3);

        let mut right = RoaringBitmap::new();
        right.insert(2);
        right.insert(4);

        let result = intersect(
            &PredicateResult::Bitmap(left),
            &PredicateResult::Bitmap(right),
        );
        match result {
            PredicateResult::Bitmap(bitmap) => {
                assert!(bitmap.contains(2));
                assert!(!bitmap.contains(1));
            }
            _ => panic!("expected bitmap"),
        }
    }

    #[test]
    fn test_union_predicate_result() {
        let left = PredicateResult::PageRanges(vec![PageRange::new(0, 5)]);
        let right = PredicateResult::PageRanges(vec![PageRange::new(3, 7)]);
        let result = union(&left, &right);
        assert_eq!(
            result,
            PredicateResult::PageRanges(vec![PageRange::new(0, 7)])
        );
    }

    #[test]
    fn test_encode_decode_ranges() {
        let ranges = vec![PageRange::new(1, 3), PageRange::new(10, 20)];
        let data = encode_page_ranges(&ranges);
        let decoded = decode_page_ranges(&data).unwrap();
        assert_eq!(ranges, decoded);
    }
}
