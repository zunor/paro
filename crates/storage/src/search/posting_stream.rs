// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared posting candidate stream protocol for inverted-index providers.

use paro_common::error::Result;

use super::budget::ResourceBudget;
use super::cursor::CandidateBatch;

pub type SearchScore = f32;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct PostingPruningHint {
    pub min_competitive_score: Option<SearchScore>,
    pub remaining_limit: Option<usize>,
}

impl PostingPruningHint {
    pub const fn new(
        min_competitive_score: Option<SearchScore>,
        remaining_limit: Option<usize>,
    ) -> Self {
        Self {
            min_competitive_score,
            remaining_limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CandidateStreamStep {
    Batch(CandidateBatch),
    Exhausted,
}

impl CandidateStreamStep {
    pub const fn is_exhausted(&self) -> bool {
        matches!(self, Self::Exhausted)
    }
}

pub trait PostingCandidateStream {
    fn is_exhausted(&self) -> bool;

    fn next_candidates(
        &mut self,
        budget: &mut ResourceBudget,
        hint: PostingPruningHint,
    ) -> Result<CandidateStreamStep>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_stream_step_reports_exhausted() {
        assert!(CandidateStreamStep::Exhausted.is_exhausted());
        assert!(!CandidateStreamStep::Batch(CandidateBatch::default()).is_exhausted());
    }

    #[test]
    fn posting_pruning_hint_carries_topk_contract() {
        let hint = PostingPruningHint::new(Some(0.42), Some(8));
        assert_eq!(hint.min_competitive_score, Some(0.42));
        assert_eq!(hint.remaining_limit, Some(8));
    }
}
