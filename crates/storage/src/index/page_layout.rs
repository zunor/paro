// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Page Layout
//!
//! Mapping from page IDs to row ranges, enabling conversion
//! between page-level and row-level index results.
//!
//! ## Usage
//!
//! `PageLayout` is constructed from the ordinal index or segment metadata
//! when creating an `IndexEvaluator`. It allows `PredicateResult::PageRanges`
//! to be precisely converted to row bitmaps for intersection with
//! `PredicateResult::Bitmap`.

use roaring::RoaringBitmap;

use super::predicate_result::PageRange;

/// Mapping from page indices to row ranges.
///
/// Each entry represents a page with its start row (inclusive) and end row (exclusive).
/// This information typically comes from the ordinal index or segment metadata.
#[derive(Debug, Clone)]
pub struct PageLayout {
    /// Per-page (start_row, end_row) pairs, ordered by page index.
    pages: Vec<PageRange>,
}

impl PageLayout {
    /// Create a new page layout from a list of page row ranges.
    ///
    /// Each entry is a `PageRange { start_row, end_row }` for page `i`.
    pub fn new(pages: Vec<PageRange>) -> Self {
        PageLayout { pages }
    }

    /// Create a layout from rows-per-page count.
    ///
    /// All pages have the same number of rows except possibly the last one.
    pub fn from_rows_per_page(rows_per_page: u32, total_rows: u32) -> Self {
        let mut pages = Vec::new();
        let mut start = 0u32;
        while start < total_rows {
            let end = (start + rows_per_page).min(total_rows);
            pages.push(PageRange::new(start, end));
            start = end;
        }
        PageLayout { pages }
    }

    /// Number of pages.
    pub fn num_pages(&self) -> usize {
        self.pages.len()
    }

    /// Get the row range for a page index.
    pub fn page_range(&self, page_idx: usize) -> Option<&PageRange> {
        self.pages.get(page_idx)
    }

    /// Convert a set of page ranges to a `RoaringBitmap` of row IDs.
    ///
    /// For each `PageRange` in the input, all row IDs within
    /// `[start_row, end_row)` are added to the bitmap.
    pub fn page_ranges_to_bitmap(&self, ranges: &[PageRange]) -> RoaringBitmap {
        let mut bitmap = RoaringBitmap::new();
        for range in ranges {
            if range.start_row < range.end_row {
                bitmap.insert_range(range.start_row..range.end_row);
            }
        }
        bitmap
    }

    /// Total number of rows covered by this layout.
    pub fn total_rows(&self) -> u32 {
        self.pages.last().map_or(0, |p| p.end_row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_rows_per_page() {
        let layout = PageLayout::from_rows_per_page(100, 250);
        assert_eq!(layout.num_pages(), 3);

        assert_eq!(layout.page_range(0), Some(&PageRange::new(0, 100)));
        assert_eq!(layout.page_range(1), Some(&PageRange::new(100, 200)));
        assert_eq!(layout.page_range(2), Some(&PageRange::new(200, 250)));
        assert_eq!(layout.total_rows(), 250);
    }

    #[test]
    fn test_page_ranges_to_bitmap() {
        let layout = PageLayout::from_rows_per_page(100, 300);

        // Pages 0 and 2 match
        let ranges = vec![PageRange::new(0, 100), PageRange::new(200, 300)];
        let bitmap = layout.page_ranges_to_bitmap(&ranges);

        assert!(bitmap.contains(0));
        assert!(bitmap.contains(99));
        assert!(!bitmap.contains(100));
        assert!(!bitmap.contains(199));
        assert!(bitmap.contains(200));
        assert!(bitmap.contains(299));
        assert_eq!(bitmap.len(), 200);
    }

    #[test]
    fn test_empty_layout() {
        let layout = PageLayout::from_rows_per_page(100, 0);
        assert_eq!(layout.num_pages(), 0);
        assert_eq!(layout.total_rows(), 0);
    }
}
