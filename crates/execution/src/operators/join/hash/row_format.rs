// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join-owned row format metadata.

use paro_common::types::LogicalType;
use paro_storage::row::RowFormat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashJoinRowFormat {
    name: &'static str,
    logical_types: Box<[LogicalType]>,
    key_count: usize,
    payload_count: usize,
}

impl HashJoinRowFormat {
    pub fn build_spill(
        key_types: impl IntoIterator<Item = LogicalType>,
        payload_types: impl IntoIterator<Item = LogicalType>,
        has_found_flag: bool,
    ) -> Self {
        let mut logical_types = key_types.into_iter().collect::<Vec<_>>();
        let key_count = logical_types.len();
        let payload_types = payload_types.into_iter().collect::<Vec<_>>();
        let payload_count = payload_types.len();
        logical_types.extend(payload_types);
        if has_found_flag {
            logical_types.push(LogicalType::UTinyInt);
        }
        logical_types.push(LogicalType::UBigInt);
        Self {
            name: "hash_join_build_spill",
            logical_types: logical_types.into_boxed_slice(),
            key_count,
            payload_count,
        }
    }

    pub fn probe_spill(probe_types: impl IntoIterator<Item = LogicalType>) -> Self {
        let logical_types = probe_types.into_iter().collect::<Vec<_>>();
        let payload_count = logical_types.len();
        Self {
            name: "hash_join_probe_spill",
            logical_types: logical_types.into_boxed_slice(),
            key_count: 0,
            payload_count,
        }
    }

    #[inline]
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    #[inline]
    pub fn payload_types(&self) -> &[LogicalType] {
        &self.logical_types[self.key_count..self.key_count + self.payload_count]
    }

    #[inline]
    pub fn payload_count(&self) -> usize {
        self.payload_count
    }
}

impl RowFormat for HashJoinRowFormat {
    fn name(&self) -> &'static str {
        self.name
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
    fn hash_join_row_format_metadata_stays_operator_owned() {
        let format = HashJoinRowFormat::build_spill(
            [LogicalType::Integer],
            [LogicalType::Varchar, LogicalType::Boolean],
            true,
        );
        let handle = RowFormatHandle::from_format(&format);

        assert_eq!(format.key_count(), 1);
        assert_eq!(format.payload_count(), 2);
        assert_eq!(
            format.payload_types(),
            &[LogicalType::Varchar, LogicalType::Boolean]
        );
        assert_eq!(handle.name(), "hash_join_build_spill");
        assert_eq!(
            handle.logical_types(),
            &[
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Boolean,
                LogicalType::UTinyInt,
                LogicalType::UBigInt,
            ]
        );
    }
}
