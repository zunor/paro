// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical-search attribution collected once, outside winner admission.
//!
//! Archive counts span cost epochs; frontier sizes describe the current epoch.
//! Payload bytes are exact slice payload sizes, not allocator traffic or RSS.

use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalSearchProfile {
    pub group_merges: u64,
    pub groups: Box<[PhysicalGroupProfile]>,
    pub frontiers: Box<[PhysicalFrontierProfile]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalGroupProfile {
    pub group: GroupId,
    pub proposals: u64,
    pub archived_candidates: u64,
    pub source_payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalFrontierProfile {
    pub group: GroupId,
    pub goal: OptimizationGoal,
    pub candidates: usize,
    pub demanded_sources: usize,
    pub proposals: u64,
    pub truncations: u64,
    pub high_water: usize,
}

impl Memo {
    pub fn physical_search_profile(&self) -> PhysicalSearchProfile {
        let mut archived = vec![(0_u64, 0_u64); self.groups.len()];
        for candidate in &self.winner_candidates {
            let entry = &mut archived[self.canonical_group(candidate.group).index()];
            entry.0 += 1;
            let lanes = candidate.winner.source_work.as_ref();
            entry.1 += std::mem::size_of_val(lanes) as u64;
            for lane in lanes {
                entry.1 += std::mem::size_of_val(lane.filters.as_ref()) as u64;
                entry.1 += std::mem::size_of_val(lane.retentions.as_ref()) as u64;
            }
        }
        let mut groups = Vec::new();
        let mut frontiers = Vec::new();
        for group in &self.groups {
            if self.canonical_group(group.id) != group.id {
                continue;
            }
            let (archived_candidates, source_payload_bytes) = archived[group.id.index()];
            groups.push(PhysicalGroupProfile {
                group: group.id,
                proposals: group.winner_proposals,
                archived_candidates,
                source_payload_bytes,
            });
            for (goal, frontier) in &group.winner_frontiers {
                // Opt-in exact vectors explain which continuation tradeoffs
                // retain a hot frontier. Never format these in normal builds.
                if frontier.truncations != 0
                    && tracing::enabled!(target: "paro::optimizer::frontier", tracing::Level::DEBUG)
                {
                    for winner in &frontier.candidates {
                        tracing::debug!(
                            target: "paro::optimizer::frontier",
                            group = group.id.index(),
                            ?goal,
                            candidate = winner.candidate.index(),
                            expression = winner.expression.index(),
                            cost = ?winner.cost,
                            "bounded frontier candidate"
                        );
                    }
                }
                frontiers.push(PhysicalFrontierProfile {
                    group: group.id,
                    goal: *goal,
                    candidates: frontier.candidates.len(),
                    demanded_sources: frontier.filterable_sources.len(),
                    proposals: frontier.proposals,
                    truncations: frontier.truncations,
                    high_water: frontier.high_water,
                });
            }
        }
        PhysicalSearchProfile {
            group_merges: self.group_merges,
            groups: groups.into_boxed_slice(),
            frontiers: frontiers.into_boxed_slice(),
        }
    }
}
