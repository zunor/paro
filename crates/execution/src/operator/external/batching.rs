// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionBatchPolicy {
    pub target_batch_bytes: u64,
    pub max_accumulation_bytes: u64,
    pub min_batch_rows: usize,
    pub max_batch_rows: usize,
    pub emission_chunk_rows: usize,
}

impl SubmissionBatchPolicy {
    pub fn from_dispatch_policy(policy: &ExternalDispatchPolicy) -> Self {
        Self {
            target_batch_bytes: policy.target_batch_bytes,
            max_accumulation_bytes: policy.max_accumulation_bytes,
            min_batch_rows: policy.min_batch_rows,
            max_batch_rows: policy.max_batch_rows,
            emission_chunk_rows: VECTOR_SIZE,
        }
    }

    pub fn estimate_row_bytes(types: &[LogicalType]) -> u64 {
        if types.is_empty() {
            return 8;
        }

        types
            .iter()
            .map(|logical_type| {
                let base = logical_type.type_size() as u64;
                let varlen_headroom = match logical_type {
                    LogicalType::Varchar
                    | LogicalType::VarcharCollation(_)
                    | LogicalType::Blob
                    | LogicalType::Json
                    | LogicalType::Jsonb
                    | LogicalType::TsVector
                    | LogicalType::TsQuery => 16,
                    LogicalType::List(_) => 16,
                    LogicalType::Struct(fields) => fields.len() as u64 * 8,
                    LogicalType::Array(_, array_size) => (*array_size as u64).min(8) * 4,
                    _ => 0,
                };
                base + varlen_headroom + 1
            })
            .sum::<u64>()
            .max(1)
    }

    pub fn estimate_chunk_bytes(chunk: &Chunk) -> u64 {
        (chunk.size() as u64).saturating_mul(Self::estimate_row_bytes(&chunk.types()))
    }

    pub fn suggest_batch_rows(&self, input_types: &[LogicalType], buffered_bytes: u64) -> usize {
        let row_bytes = Self::estimate_row_bytes(input_types);
        let budget = self.target_batch_bytes.min(
            self.max_accumulation_bytes
                .saturating_sub(buffered_bytes)
                .max(row_bytes),
        );
        let rows = (budget / row_bytes).max(self.min_batch_rows as u64);
        rows.clamp(self.min_batch_rows as u64, self.max_batch_rows as u64) as usize
    }

    pub fn should_flush(
        &self,
        buffered_rows: usize,
        buffered_bytes: u64,
        tail_flush: bool,
        input_types: &[LogicalType],
    ) -> bool {
        if buffered_rows == 0 {
            return false;
        }
        if tail_flush {
            return true;
        }

        buffered_bytes >= self.max_accumulation_bytes
            || buffered_rows >= self.suggest_batch_rows(input_types, buffered_bytes)
    }

    pub fn rechunk_output(
        &self,
        chunk: &Chunk,
        allocator: Arc<dyn Allocator>,
    ) -> Result<VecDeque<Chunk>> {
        let mut batches = VecDeque::new();
        if chunk.is_empty() {
            return Ok(batches);
        }

        if chunk.size() <= self.emission_chunk_rows {
            batches.push_back(chunk.try_deep_copy(allocator)?);
            return Ok(batches);
        }

        let mut offset = 0;
        while offset < chunk.size() {
            let take = (chunk.size() - offset).min(self.emission_chunk_rows);
            let mut slice = chunk.clone();
            slice.try_slice_range(offset, take)?;
            batches.push_back(slice.try_deep_copy(allocator.clone())?);
            offset += take;
        }

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::SubmissionBatchPolicy;
    use paro_common::allocator::default_allocator;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::VECTOR_SIZE;
    use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;
    use std::sync::Arc;

    #[test]
    fn batch_rows_follow_byte_budget() {
        let policy = SubmissionBatchPolicy::from_dispatch_policy(&ExternalDispatchPolicy {
            target_batch_bytes: 64,
            max_accumulation_bytes: 256,
            min_batch_rows: 1,
            max_batch_rows: 1024,
            max_queue_depth_per_shard: 8,
            local_spin_budget_us: 50,
            worker_acquire_timeout_ms: 500,
            transport_retry_budget: 1,
        });

        let rows = policy.suggest_batch_rows(&[LogicalType::BigInt, LogicalType::Varchar], 0);
        assert!(rows < 8);
        assert!(rows >= 1);
    }

    #[test]
    fn rechunk_output_uses_engine_friendly_chunk_size() {
        let allocator = Arc::new(default_allocator());
        let values = (0..(VECTOR_SIZE as i32 + 10)).collect::<Vec<_>>();
        let chunk = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &values,
                allocator.clone(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let policy =
            SubmissionBatchPolicy::from_dispatch_policy(&ExternalDispatchPolicy::default());

        let batches = policy.rechunk_output(&chunk, allocator).unwrap();
        assert_eq!(batches.len(), 2);
        assert!(batches.front().expect("first batch").size() <= VECTOR_SIZE);
        assert!(batches.back().expect("second batch").size() <= VECTOR_SIZE);
    }
}
