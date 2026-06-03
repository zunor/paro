// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sort-owned row format metadata.

use paro_common::types::LogicalType;
use paro_storage::row::RowFormat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortRowFormat {
    logical_types: Box<[LogicalType]>,
    key_width: usize,
}

impl SortRowFormat {
    pub fn new(
        key_types: impl IntoIterator<Item = LogicalType>,
        payload_types: impl IntoIterator<Item = LogicalType>,
    ) -> Self {
        let mut logical_types = key_types.into_iter().collect::<Vec<_>>();
        let key_width = logical_types.len();
        logical_types.extend(payload_types);
        Self {
            logical_types: logical_types.into_boxed_slice(),
            key_width,
        }
    }

    #[inline]
    pub fn key_width(&self) -> usize {
        self.key_width
    }
}

impl RowFormat for SortRowFormat {
    fn name(&self) -> &'static str {
        "sort_run"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }
}

#[cfg(test)]
mod tests {
    use paro_storage::row::RowFormatHandle;

    use super::*;

    #[test]
    fn sort_row_format_metadata_stays_operator_owned() {
        let format = SortRowFormat::new([LogicalType::Integer], [LogicalType::Varchar]);
        let handle = RowFormatHandle::from_format(&format);

        assert_eq!(format.key_width(), 1);
        assert_eq!(handle.name(), "sort_run");
        assert_eq!(
            handle.logical_types(),
            &[LogicalType::Integer, LogicalType::Varchar]
        );
    }
}
