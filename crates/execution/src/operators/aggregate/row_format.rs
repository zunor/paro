// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate-owned row format metadata.

use paro_common::types::LogicalType;
use paro_storage::row::RowFormat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateGroupFormat {
    logical_types: Box<[LogicalType]>,
    group_width: usize,
    state_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatePayloadFormat {
    logical_types: Box<[LogicalType]>,
    payload_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateStateFormat {
    logical_types: Box<[LogicalType]>,
    group_width: usize,
    state_width: usize,
}

impl AggregateGroupFormat {
    pub fn new(group_types: impl IntoIterator<Item = LogicalType>, state_count: usize) -> Self {
        let group_types = group_types.into_iter().collect::<Vec<_>>();
        let group_width = group_types.len();
        Self {
            logical_types: group_types.into_boxed_slice(),
            group_width,
            state_count,
        }
    }

    pub fn finalized_output(
        logical_types: impl IntoIterator<Item = LogicalType>,
        group_width: usize,
        state_count: usize,
    ) -> Self {
        Self {
            logical_types: logical_types
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            group_width,
            state_count,
        }
    }

    #[inline]
    pub fn group_width(&self) -> usize {
        self.group_width
    }

    #[inline]
    pub fn state_count(&self) -> usize {
        self.state_count
    }
}

impl AggregatePayloadFormat {
    pub const HASH_COL_IDX: usize = 0;

    pub fn new(payload_types: impl IntoIterator<Item = LogicalType>) -> Self {
        let payload_types = payload_types.into_iter().collect::<Vec<_>>();
        let mut logical_types = Vec::with_capacity(payload_types.len() + 1);
        logical_types.push(LogicalType::UBigInt);
        logical_types.extend(payload_types.iter().cloned());
        Self {
            logical_types: logical_types.into_boxed_slice(),
            payload_width: payload_types.len(),
        }
    }

    #[inline]
    pub fn payload_width(&self) -> usize {
        self.payload_width
    }

    #[inline]
    pub fn payload_types(&self) -> &[LogicalType] {
        &self.logical_types[1..]
    }
}

impl AggregateStateFormat {
    pub const HASH_COL_IDX: usize = 0;

    pub fn new(group_types: impl IntoIterator<Item = LogicalType>, state_width: usize) -> Self {
        let group_types = group_types.into_iter().collect::<Vec<_>>();
        let mut logical_types = Vec::with_capacity(group_types.len() + 2);
        logical_types.push(LogicalType::UBigInt);
        logical_types.extend(group_types.iter().cloned());
        logical_types.push(LogicalType::Blob);
        Self {
            logical_types: logical_types.into_boxed_slice(),
            group_width: group_types.len(),
            state_width,
        }
    }

    #[inline]
    pub fn group_width(&self) -> usize {
        self.group_width
    }

    #[inline]
    pub fn state_width(&self) -> usize {
        self.state_width
    }

    #[inline]
    pub fn group_types(&self) -> &[LogicalType] {
        &self.logical_types[1..1 + self.group_width]
    }

    #[inline]
    pub fn state_col_idx(&self) -> usize {
        1 + self.group_width
    }
}

impl RowFormat for AggregateGroupFormat {
    fn name(&self) -> &'static str {
        "aggregate_group"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }
}

impl RowFormat for AggregatePayloadFormat {
    fn name(&self) -> &'static str {
        "aggregate_payload"
    }

    fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }
}

impl RowFormat for AggregateStateFormat {
    fn name(&self) -> &'static str {
        "aggregate_state"
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
    fn aggregate_group_format_metadata_stays_operator_owned() {
        let format = AggregateGroupFormat::new([LogicalType::Integer], 2);
        let handle = RowFormatHandle::from_format(&format);

        assert_eq!(format.group_width(), 1);
        assert_eq!(format.state_count(), 2);
        assert_eq!(handle.name(), "aggregate_group");
        assert_eq!(handle.logical_types(), &[LogicalType::Integer]);
    }

    #[test]
    fn aggregate_group_format_can_describe_finalized_output_rows() {
        let format = AggregateGroupFormat::finalized_output(
            [LogicalType::Integer, LogicalType::BigInt],
            1,
            1,
        );
        let handle = RowFormatHandle::from_format(&format);

        assert_eq!(format.group_width(), 1);
        assert_eq!(format.state_count(), 1);
        assert_eq!(
            handle.logical_types(),
            &[LogicalType::Integer, LogicalType::BigInt]
        );
    }

    #[test]
    fn aggregate_payload_format_prefixes_hash_column() {
        let format = AggregatePayloadFormat::new([LogicalType::Integer, LogicalType::Varchar]);
        let handle = RowFormatHandle::from_format(&format);

        assert_eq!(format.payload_width(), 2);
        assert_eq!(
            format.payload_types(),
            &[LogicalType::Integer, LogicalType::Varchar]
        );
        assert_eq!(handle.name(), "aggregate_payload");
        assert_eq!(
            handle.logical_types(),
            &[
                LogicalType::UBigInt,
                LogicalType::Integer,
                LogicalType::Varchar
            ]
        );
    }

    #[test]
    fn aggregate_state_format_prefixes_hash_and_suffixes_state_blob() {
        let format = AggregateStateFormat::new([LogicalType::Integer], 16);
        let handle = RowFormatHandle::from_format(&format);

        assert_eq!(format.group_width(), 1);
        assert_eq!(format.state_width(), 16);
        assert_eq!(format.group_types(), &[LogicalType::Integer]);
        assert_eq!(format.state_col_idx(), 2);
        assert_eq!(handle.name(), "aggregate_state");
        assert_eq!(
            handle.logical_types(),
            &[
                LogicalType::UBigInt,
                LogicalType::Integer,
                LogicalType::Blob
            ]
        );
    }
}
