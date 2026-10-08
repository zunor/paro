// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Accounted normalized-key sorting for domains with a total scalar order.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryError, MemoryResult,
};
use paro_common::sort_key::{OrderModifiers, SortKeyEncoding};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_context::StatementCancellation;
use paro_planner::expression::{Expression, WindowExpression};

use super::{expression_column, partition_type_matches_order, window_metadata, WindowRowKey};

#[derive(Clone, Copy)]
struct EncodedRow {
    key: WindowRowKey,
    ordinal: usize,
    start: usize,
    partition_end: usize,
    end: usize,
}

/// Encodings concatenate self-delimiting fields, so equal partition prefixes
/// identify complete partitions in a monotone input. Retain the grouping
/// advantage instead of repeatedly comparing those prefixes in a global sort.
/// The returned flag also lets tests verify the selected sorting strategy.
fn sort_encoded_rows(
    rows: &mut [EncodedRow],
    bytes: &[u8],
    cancel: &StatementCancellation,
) -> Result<bool> {
    let mut partition_ordered = true;
    for (index, pair) in rows.windows(2).enumerate() {
        if index % VECTOR_SIZE == 0 {
            cancel.check()?;
        }
        if bytes[pair[0].start..pair[0].partition_end] > bytes[pair[1].start..pair[1].partition_end]
        {
            partition_ordered = false;
            break;
        }
    }
    if partition_ordered {
        let mut start = 0;
        while start < rows.len() {
            cancel.check()?;
            let prefix = &bytes[rows[start].start..rows[start].partition_end];
            let mut end = start + 1;
            while end < rows.len() && prefix == &bytes[rows[end].start..rows[end].partition_end] {
                if end % VECTOR_SIZE == 0 {
                    cancel.check()?;
                }
                end += 1;
            }
            rows[start..end].sort_unstable_by(|a, b| {
                bytes[a.partition_end..a.end]
                    .cmp(&bytes[b.partition_end..b.end])
                    .then_with(|| a.ordinal.cmp(&b.ordinal))
            });
            start = end;
        }
    } else {
        cancel.check()?;
        rows.sort_unstable_by(|a, b| {
            bytes[a.start..a.end]
                .cmp(&bytes[b.start..b.end])
                .then_with(|| a.ordinal.cmp(&b.ordinal))
        });
    }
    Ok(partition_ordered)
}

/// Cache scratch is optional. Capacity/physical allocation failures leave the
/// input untouched and select the existing comparison sort. Reclaim errors and
/// blocked progress retain their error semantics.
fn cache_allocation<T>(result: MemoryResult<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(
            MemoryError::QuotaExhausted { .. }
            | MemoryError::RuntimeCapExhausted { .. }
            | MemoryError::PhysicalAllocationFailed { .. },
        ) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn try_sort_encoded(
    chunks: &[Chunk],
    keys: &mut AccountedVec<WindowRowKey>,
    expr: &WindowExpression,
    cancel: &StatementCancellation,
) -> Result<bool> {
    // Small domains do not amortize the extra encoding and metadata pass.
    if keys.len() < VECTOR_SIZE {
        return Ok(false);
    }
    cancel.check()?;
    let mut columns = Vec::new();
    let mut types = Vec::new();
    let mut modifiers = Vec::new();
    let mut partition_columns = 0;
    for (index, (expression, modifier)) in expr
        .partitions
        .iter()
        .map(|p| (p, OrderModifiers::new(true, false)))
        .chain(expr.orders.iter().map(|o| {
            (
                &o.expression,
                OrderModifiers::new(o.ascending, o.nulls_first),
            )
        }))
        .enumerate()
    {
        let Some(column) = expression_column(expression) else {
            if matches!(expression, Expression::Constant(_)) {
                continue;
            }
            return Ok(false);
        };
        let ty = expression.return_type();
        // The existing comparator treats NaN as equal and signed zero as
        // peers. Normalized floating keys have a different order. Nested and
        // other fallback Value domains also retain their existing comparator.
        if !partition_type_matches_order(ty.clone()) && !matches!(ty, LogicalType::Decimal { .. }) {
            return Ok(false);
        }
        columns.push(column);
        types.push(ty);
        modifiers.push(modifier);
        if index < expr.partitions.len() {
            partition_columns += 1;
        }
    }
    if columns.is_empty() {
        return Ok(true);
    }
    let partition_encoding = SortKeyEncoding::new(
        types[..partition_columns].to_vec(),
        modifiers[..partition_columns].to_vec(),
    )?;
    let encoding = SortKeyEncoding::new(types, modifiers)?;
    for chunk in chunks {
        encoding.validate_columns(chunk, &columns)?;
    }
    let memory = MemoryAccountingContext::new(
        keys.grant().owner(),
        keys.grant().domain(),
        MemoryTag::BaseTable,
        MemoryAccountingClass::NonRevocable,
    );
    let mut rows = window_metadata(&memory)?;
    if cache_allocation(rows.try_reserve(keys.len()))?.is_none() {
        cancel.check()?;
        return Ok(false);
    }
    let mut total = 0usize;
    for (ordinal, &key) in keys.iter().enumerate() {
        if ordinal % VECTOR_SIZE == 0 {
            cancel.check()?;
        }
        let len = encoding.encoded_len_trusted(&chunks[key.chunk_idx], key.row_idx, &columns)?;
        let end = total
            .checked_add(len)
            .ok_or_else(|| paro_error::internal("window encoded key size overflow"))?;
        let partition_len = partition_encoding.encoded_len_trusted(
            &chunks[key.chunk_idx],
            key.row_idx,
            &columns[..partition_columns],
        )?;
        debug_assert!(partition_len <= len);
        rows.try_push(EncodedRow {
            key,
            ordinal,
            start: total,
            partition_end: total + partition_len,
            end,
        })?;
        total = end;
    }
    let mut bytes = window_metadata(&memory)?;
    if cache_allocation(bytes.try_resize_with(total, || 0u8))?.is_none() {
        cancel.check()?;
        return Ok(false);
    }
    for row in rows.iter() {
        if row.ordinal % VECTOR_SIZE == 0 {
            cancel.check()?;
        }
        let encoded = &mut bytes[row.start..row.end];
        let inline = encoded.len().min(encoding.inline_prefix_len());
        let (prefix, overflow) = encoded.split_at_mut(inline);
        encoding.encode_row_into_parts_trusted(
            &chunks[row.key.chunk_idx],
            row.key.row_idx,
            &columns,
            prefix,
            overflow,
        )?;
    }
    cancel.check()?;
    // An ordinal tie breaker preserves the previous stable sort's peer order,
    // while the unstable sort needs no additional unaccounted scratch buffer.
    sort_encoded_rows(&mut rows, &bytes, cancel)?;
    cancel.check()?;
    for (key, row) in keys.iter_mut().zip(rows.iter()) {
        *key = row.key;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_rows(fields: &[(&[u8], &[u8])]) -> (Vec<EncodedRow>, Vec<u8>) {
        let mut rows = Vec::new();
        let mut bytes = Vec::new();
        for (ordinal, (partition, order)) in fields.iter().enumerate() {
            let start = bytes.len();
            bytes.extend_from_slice(partition);
            let partition_end = bytes.len();
            bytes.extend_from_slice(order);
            rows.push(EncodedRow {
                key: WindowRowKey {
                    chunk_idx: 0,
                    row_idx: ordinal,
                },
                ordinal,
                start,
                partition_end,
                end: bytes.len(),
            });
        }
        (rows, bytes)
    }

    #[test]
    fn encoded_sort_selects_complete_partitions_or_global_fallback() {
        let cancel = StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None);
        // Partitions share a long prefix; comparison must use the whole field.
        // Repeated suffixes retain their input ordinal inside each partition.
        let fields: &[(&[u8], &[u8])] = &[
            (b"long-prefix-a\0", b"z"),
            (b"long-prefix-a\0", b"a"),
            (b"long-prefix-a\0", b"a"),
            (b"long-prefix-ab\0", b"z"),
            (b"long-prefix-ab\0", b"a"),
            (b"long-prefix-ab\0", b"a"),
        ];
        let (mut rows, bytes) = encoded_rows(fields);
        assert!(sort_encoded_rows(&mut rows, &bytes, &cancel).unwrap());
        assert_eq!(
            rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            [1, 2, 0, 4, 5, 3]
        );

        let (mut rows, bytes) = encoded_rows(fields);
        rows.reverse();
        assert!(!sort_encoded_rows(&mut rows, &bytes, &cancel).unwrap());
        assert_eq!(
            rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            [1, 2, 0, 4, 5, 3]
        );
    }

    #[test]
    fn encoded_sort_handles_empty_partition_or_order_prefix_and_cancellation() {
        let token = tokio_util::sync::CancellationToken::new();
        let cancel = StatementCancellation::new(token.clone(), None);
        let (mut rows, bytes) = encoded_rows(&[(b"", b"z"), (b"", b"a"), (b"", b"a")]);
        assert!(sort_encoded_rows(&mut rows, &bytes, &cancel).unwrap());
        assert_eq!(
            rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            [1, 2, 0]
        );
        let (mut rows, bytes) = encoded_rows(&[(b"a\0", b""), (b"a\0", b""), (b"ab\0", b"")]);
        assert!(sort_encoded_rows(&mut rows, &bytes, &cancel).unwrap());
        assert_eq!(
            rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        token.cancel();
        assert!(sort_encoded_rows(&mut rows, &bytes, &cancel).is_err());
        assert_eq!(
            rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }
}
