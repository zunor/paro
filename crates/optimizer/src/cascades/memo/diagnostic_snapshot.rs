// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded, opt-in P1 snapshot. Capture immutable handles after search; serialize
//! after the compiler has stopped its optimizer timer. No search decision reads
//! this state. This is diagnostic preparation, not a normal C1 optimization.

use super::*;
use std::cell::RefCell;
use std::io::Write;

const LIMIT: usize = 16_384;
struct Frontier {
    group: GroupId,
    goal: OptimizationGoal,
    high_water: usize,
    truncations: u64,
    sources: BTreeSet<crate::cost::response::WorkSourceId>,
    candidates: Vec<Arc<Winner>>,
}
struct Snapshot {
    frontiers: Vec<Frontier>,
    published: u64,
    omitted: usize,
    capture_us: u128,
}
thread_local! {
    static PENDING: RefCell<Option<Snapshot>> = const { RefCell::new(None) };
}

/// Synchronous optimizer invocation owns the thread-local diagnostic slot.
pub fn clear() {
    PENDING.with(|slot| {
        slot.borrow_mut().take();
    });
}

pub(super) fn capture(memo: &Memo) {
    if std::env::var_os("PARO_DIAGNOSTIC_FRONTIER_SNAPSHOT").is_none() {
        return;
    }
    PENDING.with(|slot| *slot.borrow_mut() = Some(collect(memo)));
}

fn collect(memo: &Memo) -> Snapshot {
    let started = std::time::Instant::now();
    let mut frontiers = Vec::new();
    let mut count = 0;
    let mut omitted = 0;
    for group in &memo.groups {
        if memo.canonical_group(group.id) != group.id {
            continue;
        }
        for (goal, frontier) in &group.winner_frontiers {
            if count + frontier.candidates.len() > LIMIT || frontiers.len() == LIMIT {
                omitted += frontier.candidates.len();
                continue;
            }
            count += frontier.candidates.len();
            frontiers.push(Frontier {
                group: group.id,
                goal: *goal,
                high_water: frontier.high_water,
                truncations: frontier.truncations,
                sources: frontier.filterable_sources.clone(),
                candidates: frontier.candidates.clone(),
            });
        }
    }
    Snapshot {
        frontiers,
        published: memo.published_winner_count(),
        omitted,
        capture_us: started.elapsed().as_micros(),
    }
}

#[cfg(test)]
pub(super) fn counts(memo: &Memo) -> (u64, usize, usize) {
    let snapshot = collect(memo);
    (
        snapshot.published,
        snapshot.frontiers.iter().map(|f| f.candidates.len()).sum(),
        snapshot.omitted,
    )
}

/// Called after recording optimizer elapsed time. File I/O remains inside the
/// diagnostic statement's compiler/C1 lifecycle and is never called a saving.
pub fn flush(statement: &str) -> std::io::Result<()> {
    let Some(snapshot) = PENDING.with(|slot| slot.borrow_mut().take()) else {
        return Ok(());
    };
    let Some(path) = std::env::var_os("PARO_DIAGNOSTIC_FRONTIER_SNAPSHOT") else {
        return Ok(());
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let mut rows = Vec::new();
    for frontier in snapshot.frontiers {
        // Equality is the exact production response relation, not a hash.
        let mut representatives: Vec<&Winner> = Vec::new();
        let mut candidates = Vec::new();
        for winner in &frontier.candidates {
            let response = representatives
                .iter()
                .position(|other| source_response_equal(winner, other, &frontier.sources))
                .unwrap_or_else(|| {
                    representatives.push(winner);
                    representatives.len() - 1
                });
            let c = winner.cost;
            candidates.push(serde_json::json!({
                "candidate": winner.candidate.index(), "physical": winner.expression.index(),
                "fingerprint": format!("{:?}", winner.physical_fingerprint),
                "expected": c.score.range.expected, "risk": c.score.risk_adjusted,
                "work": c.work_latency.expected, "span": c.critical_path.expected,
                "non_revocable": c.non_revocable_memory_upper,
                "minimum": c.minimum_memory_bytes, "preferred": c.preferred_memory_bytes(),
                "peak": c.peak_memory_upper, "completion": format!("{:?}", c.memory_completion),
                "spill": c.spill_bytes_expected, "external_slots": c.external_worker_slots_upper,
                "tasks": c.max_parallel_tasks, "output_tasks": c.output_pipeline_tasks,
                "external_workers": format!("{:?}", c.external_workers),
                "source_response_class": response,
                "source_count": winner.source_work.iter().filter(|s| frontier.sources.contains(&s.source)).count(),
                "children": winner.children.iter().map(|c| c.candidate.index()).collect::<Vec<_>>(),
            }));
        }
        rows.push(serde_json::json!({
            "group": frontier.group.index(), "goal": format!("{:?}", frontier.goal),
            "high_water": frontier.high_water, "truncations": frontier.truncations,
            "demanded_sources": frontier.sources.len(), "candidates": candidates,
        }));
    }
    serde_json::to_writer(
        &mut file,
        &serde_json::json!({
            "schema": 1, "pid": std::process::id(), "statement": statement,
            "published_archive": snapshot.published, "omitted_candidates": snapshot.omitted,
            "capture_us": snapshot.capture_us, "frontiers": rows,
            "replacement_and_invalidation_counts": null,
        }),
    )?;
    writeln!(file)
}
