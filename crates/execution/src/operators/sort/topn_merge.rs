// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Failure-atomic merge of task-local TopN frontiers.

use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct CombineCandidate {
    source_index: usize,
    heap_position: usize,
    old_payload_index: usize,
    new_payload_index: usize,
}

impl TopNHeap {
    /// Combine another heap into this one.
    ///
    /// Used to merge results from parallel sinks.
    pub fn combine(&mut self, other: &mut TopNHeap) -> Result<()> {
        self.combine_many(std::slice::from_mut(other))
    }

    /// Merge a bounded worker frontier in one selection and copy pass.
    ///
    /// All source heaps stay intact until the final candidate payload has been
    /// admitted and materialized, preserving the same failure atomicity as a
    /// two-heap combine without repeatedly copying the incumbent top set.
    pub(crate) fn combine_many(&mut self, others: &mut [TopNHeap]) -> Result<()> {
        for other in others.iter() {
            if self.heap_size != other.heap_size
                || self.offset != other.offset
                || self.modifiers != other.modifiers
                || self.payload_types != other.payload_types
                || !self.memory.has_same_target(&other.memory)
            {
                return Err(paro_common::error::internal(
                    "cannot combine incompatible TopN heaps",
                ));
            }
        }

        let candidate_count = others.iter().try_fold(self.heap.len(), |count, other| {
            count
                .checked_add(other.heap.len())
                .ok_or_else(|| paro_common::error::internal("TopN candidate count overflow"))
        })?;
        let mut candidates = accounted_metadata_vec(&self.memory);
        candidates.try_reserve(candidate_count)?;
        append_candidates(&mut candidates, 0, &self.heap)?;
        for (other_index, other) in others.iter().enumerate() {
            append_candidates(&mut candidates, other_index + 1, &other.heap)?;
        }

        let compare_candidates = |left: &CombineCandidate, right: &CombineCandidate| {
            candidate_entry(left, &self.heap, others)
                .sort_key
                .as_slice()
                .cmp(
                    candidate_entry(right, &self.heap, others)
                        .sort_key
                        .as_slice(),
                )
                .then_with(|| left.source_index.cmp(&right.source_index))
                .then_with(|| left.old_payload_index.cmp(&right.old_payload_index))
        };
        if candidates.len() > self.heap_size {
            candidates
                .as_mut_slice()
                .select_nth_unstable_by(self.heap_size, compare_candidates);
            candidates.truncate(self.heap_size);
        }
        candidates
            .sort_unstable_by_key(|candidate| (candidate.source_index, candidate.heap_position));
        for (new_payload_index, candidate) in candidates.iter_mut().enumerate() {
            candidate.new_payload_index = new_payload_index;
        }

        let expected_entries = candidates.len();
        let mut final_heap = TopNEntryHeap::try_with_capacity(&self.memory, expected_entries)?;
        let staged_data = self.gather_candidate_payloads(&candidates, others)?;

        // Copy is complete. Transfer key ownership and publish the new address
        // domain without cloning keys or allocating more metadata.
        let mut candidate_index = 0usize;
        transfer_entries(
            &mut self.heap,
            0,
            &candidates,
            &mut candidate_index,
            &mut final_heap,
        );
        for (other_index, other) in others.iter_mut().enumerate() {
            transfer_entries(
                &mut other.heap,
                other_index + 1,
                &candidates,
                &mut candidate_index,
                &mut final_heap,
            );
        }
        debug_assert_eq!(final_heap.len(), expected_entries);
        self.heap = final_heap;
        self.heap_data = staged_data;
        for other in others {
            other.heap = TopNEntryHeap::new(&other.memory);
            other.heap_data = RetainedChunkVec::new(other.memory.clone());
        }
        Ok(())
    }

    fn gather_candidate_payloads(
        &self,
        candidates: &[CombineCandidate],
        others: &[TopNHeap],
    ) -> Result<RetainedChunkVec> {
        let mut staged_data = RetainedChunkVec::new(self.memory.clone());
        for source_index in 0..=others.len() {
            let start =
                candidates.partition_point(|candidate| candidate.source_index < source_index);
            let end =
                candidates.partition_point(|candidate| candidate.source_index <= source_index);
            if start == end {
                continue;
            }
            let source_data = if source_index == 0 {
                self.heap_data.as_slice()
            } else {
                others[source_index - 1].heap_data.as_slice()
            };
            Self::for_each_gathered_rows(
                &self.memory,
                &self.payload_types,
                source_data,
                end - start,
                |row| candidates[start + row].old_payload_index,
                |chunk| {
                    staged_data.push(chunk)?;
                    Ok(())
                },
            )?;
        }
        Ok(staged_data)
    }
}

fn append_candidates(
    candidates: &mut AccountedVec<CombineCandidate>,
    source_index: usize,
    heap: &TopNEntryHeap,
) -> Result<()> {
    for (heap_position, entry) in heap.iter().enumerate() {
        candidates.try_push(CombineCandidate {
            source_index,
            heap_position,
            old_payload_index: entry.index,
            new_payload_index: 0,
        })?;
    }
    Ok(())
}

fn transfer_entries(
    source: &mut TopNEntryHeap,
    source_index: usize,
    candidates: &[CombineCandidate],
    candidate_index: &mut usize,
    target: &mut TopNEntryHeap,
) {
    for (heap_position, mut entry) in source.drain().enumerate() {
        let Some(candidate) = candidates.get(*candidate_index) else {
            break;
        };
        if candidate.source_index != source_index || candidate.heap_position != heap_position {
            continue;
        }
        entry.index = candidate.new_payload_index;
        target.push_prepared(entry);
        *candidate_index += 1;
    }
}

fn candidate_entry<'a>(
    candidate: &CombineCandidate,
    target: &'a TopNEntryHeap,
    others: &'a [TopNHeap],
) -> &'a TopNEntry {
    if candidate.source_index == 0 {
        &target.as_slice()[candidate.heap_position]
    } else {
        &others[candidate.source_index - 1].heap.as_slice()[candidate.heap_position]
    }
}
