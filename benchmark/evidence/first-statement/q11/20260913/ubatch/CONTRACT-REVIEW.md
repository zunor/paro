# U-BATCH contract review — NOT MEASURED U

Status: read-only source investigation plus this document and an independent
oracle artifact. No optimizer implementation, build, test, oracle execution or
benchmark was performed for this delivery. Reviewed working-tree source at HEAD
`7e2503493c2d3740bf68224993e5e8177be414f5`; unrelated user changes are preserved.
Line references below describe that inspected source, not an assertion that the
whole working tree was clean.

## Decision needed before implementation

Whole-physical-expression atomic admission/publication is **not equivalent** to
the current prefix contract. Implementing a prefix fallback would address the
counterexample, but changes the proposal into **bounded-prefix microbatches**;
it must not be presented as fulfillment of the original whole-expression batch.

The user must choose:

1. Bounded-prefix microbatches / frozen-visit preparation reuse, preserving every
   tuple admission, exact comparison, publication, stop/yield and resume boundary.
2. Atomic-expression/deadline semantics, explicitly authorizing changed prefix
   availability, budget outcomes, handoff timing and potentially synthesis count.

Option 2 cannot simultaneously promise the existing exact budget/prefix contract.
No implementation of either option is authorized by this review alone.

## Minimal counterexample: mandatory baseline is already done

One immutable physical recipe and goal; one child has a selected mandatory
baseline and two additional, active, unpriced, resource-feasible candidate tuples
`a` and `b`. Both optional event identities are fresh. Mandatory baseline is
already published and excluded from all counts below. Remaining optional
`ChildFrontierCombination` credit is **1**. Baseline cost is 100; tuple costs are
10 and 5, with identical other comparison axes. No timeout, cancellation, fact
change, grant change, shared-CTE complication or rule error is needed.

| Operation | Current per-tuple path | Whole optional batch of size 2 |
|---|---|---|
| Admission | admit `a` (1 unit), then reject `b` | reject batch (2 > 1) |
| Optional credit consumed | 1 | 0 |
| New synthesis count | 1 | 0 |
| Published prefix | `a` | none |
| Incumbent | 10 | baseline 100 |
| Omitted work | `b`, retained for retry | whole batch |

This cannot be dismissed by "publish mandatory baseline first": that has already
happened on both sides. Reserving two units before synthesis rejects the entire
batch; synthesizing both before admission performs work the current path does
not admit. Admitting the available prefix and publishing normally is a different,
prefix-microbatch contract, not whole-expression atomicity.

Production source correspondence:

- [budget.rs:402](../../../../../../crates/optimizer/src/cascades/budget.rs#L402):
  `admit_optional` requests one unit. `admit_optional_units` computes the additional
  uncharged units for the event and returns Exhausted **before reserving any**
  when that additional amount exceeds remaining credit. It does not partially
  reserve a new oversized event. Existing batch-event idempotence is not a
  license to replace the distinct tuple-event identities.
- [engine.rs:8575](../../../../../../crates/optimizer/src/cascades/engine.rs#L8575):
  nonmandatory tuple event admission precedes synthesis; exhaustion stores the
  tuple in `budget_rejected` and breaks. The synthesis increment is at 8604.
  Prior priced/published tuples are not rolled back by this exhaustion.
- [recipe_resume.rs:269](../../../../../../crates/optimizer/src/cascades/engine/tests/recipe_resume.rs#L269):
  existing Rust evidence for denied tuples having no cost, later credit recovery
  causing one synthesis, and repeated delivery consuming no second credit.
  This existing test is the 0-to-1 retry case, **not** the new exact 1-credit /
  2-new-tuples counterexample; the standalone artifact states that distinction.

The standalone [contract_oracle.py](contract_oracle.py) is a built-in-Python,
integer-arithmetic finite model. It models new optional tuples only, demonstrates
the credit counterexample, checks a bounded grid, and separately demonstrates
publication-boundary and frozen-preview failures. It does not import Paro or
claim to verify the actual Rust implementation, shared-CTE algebra, or performance.
It has **not been executed**. Future command, only after performance permission:

```sh
python3 benchmark/evidence/first-statement/q11/20260913/ubatch/contract_oracle.py
```

## Ample credit permits pure cost preparation, not a global frozen frontier

In a synchronous recipe visit, immutable child candidates and recipe fields can
support a pure cost kernel. The current implementation already hoists enforcement
and reuses several scratch vectors. This is a plausible narrow optimization
surface, not evidence of a 20 microsecond saving or a novel missing mechanism.

The following observations remain sequential even with abundant credit:

- **Pareto / truncation:** `candidate_preview` consults the current parent
  frontier; each `record_winner` can change it, archive an immutable CandidateId,
  advance its revision and retain a truncation witness. For two candidates where
  `a` dominates `b`, both can pass preview against the initial empty frontier,
  whereas sequential `a` publication makes `b` rejected. Precomputing costs then
  performing the original sequential comparisons avoids this particular error;
  comparing the entire batch against one frozen frontier does not.
  See [memo.rs:2748](../../../../../../crates/optimizer/src/cascades/memo.rs#L2748) and
  [memo.rs:2853](../../../../../../crates/optimizer/src/cascades/memo.rs#L2853).
- **Intermediate visibility:** frontier changes trigger quality inspection and
  physical-step yield. The step has a 32-publication threshold, and a waiting
  parent may perform one explicitly counted response after a child yields.
  At remaining slice allowance 1, speculatively synthesizing the second tuple
  already differs from a stop after the first publication. Existing
  [parent_response.rs:9](../../../../../../crates/optimizer/src/cascades/engine/tests/parent_response.rs#L9)
  requires 32 child publications plus one parent synthesis before the child tail.
  Pure cost calculation does not imply the whole batch may be delayed invisibly.
- **Resume and fresh deltas:** the state key is `(physical, goal, recipe fingerprint)`,
  not physical/goal alone. `observe_frontiers` adds disjoint new tuple domains;
  parent-frontier rechecks can reuse a cached cost without synthesis. Cost-context
  changes reset pricing, including rejected-resource results. Never turn a
  frontier-only recheck into a new synthesis or skip new tuples on resume.
  See [engine.rs:623](../../../../../../crates/optimizer/src/cascades/engine.rs#L623),
  [8425](../../../../../../crates/optimizer/src/cascades/engine.rs#L8425), and
  [9862](../../../../../../crates/optimizer/src/cascades/engine.rs#L9862).
- **CTE/RF/source work:** exact child CandidateIds, goal/context, source lanes,
  retention/evaluation identities, retained-state/grant feasibility and region
  artifacts remain tuple-specific. Equal group IDs or scalar scores are not a
  sufficient sharing key. A nonselected child can be the best parent response
  after RF source-work accounting. No aggregate scalar substitute for this
  evidence is established. See [engine.rs:9011](../../../../../../crates/optimizer/src/cascades/engine.rs#L9011)
  and [10164](../../../../../../crates/optimizer/src/cascades/engine.rs#L10164).

Cost-context identity explicitly covers epoch, calibration, full goal, immutable
recipe identity, canonical owner/child facts and statistics. A frozen visit cannot
silently become a persistent snapshot across yields or newly published facts.

Thus ample budget can make a bounded frozen-visit **pure calculation** possible,
but does not establish the same admission trajectory, handoff candidate or N for
an atomic whole-expression batch. Even a semantics-preserving speedup cannot
universally promise the same N under a wall deadline: more legitimate checkpoints
may be reached before expiry. Exact-count comparison requires an explicitly fixed
logical stop schedule, not an assumption that changed runtime leaves deadlines
observationally unchanged.

## Attribution: NOT MEASURED U, not a U <= 12 negative result

The prior U approximately 20 microseconds is the slope of two residual points,
`U = (R2 - R1) / (N2 - N1)`, with an affine intercept. It is not a timer around
cost synthesis or a measured per-tuple marginal function cost. Scheduling,
admission, context maintenance and other correlated work can contribute to that
slope. The contract counterexample neither measures U nor establishes U <= 12
microseconds. Record the status as **NOT MEASURED U / contract decision pending**,
not a decisive negative performance gate. No savings estimate `20us * N` follows.

## Actual existing Rust test filters for the main agent

These names were located in source; they were not run for this review. Run after
the performance window, using the common command shape below (one filter per
invocation). The older cfg(test) Cartesian batch helper is not the production
resume implementation; its test cannot alone establish the production contract.

```sh
cargo test -p paro-optimizer --lib FILTER -- --exact --nocapture
```

| Exact FILTER | Existing coverage |
|---|---|
| `cascades::budget::tests::batch_admission_is_atomic_and_idempotent` | Atomic additional-unit rejection / idempotence |
| `cascades::engine::tests::recipe_resume::recipe_resume_budget_retry_pauses_then_prices_nonselected_child_once` | Denied tuple, admitted prefix, credit retry, no duplicate synthesis |
| `cascades::engine::tests::parent_response::yielded_child_is_consumed_before_its_unexplored_tail` | Parent consumes ready prefix before child tail |
| `cascades::memo::tests::candidate_preview_matches_bounded_frontier_admission` | Dominance versus bounded-frontier truncation |
| `cascades::engine::tests::incremental_child_combination_oracle_covers_only_the_frontier_delta` | Newly published child-product delta |
| `cascades::engine::tests::recipe_resume::recipe_resume_completed_same_readset_accepts_appended_physical_recipe` | New recipe despite unchanged prior readset |
| `cascades::engine::tests::cost_identity::cached_recipe_identity_does_not_cache_child_facts_grant_or_calibration` | Immutable recipe versus changing context |
| `cascades::engine::tests::cost_identity::recipe_context_still_resolves_canonical_child_groups_after_merge` | Canonical-group refresh |
| `cascades::engine::tests::parent_costs_every_source_sensitive_child_frontier_candidate` | Nonselected child responses remain relevant |

Before any implementation is accepted, an actual engine test for the new
**remaining optional credit 1 / two new tuples / baseline already published**
case is still required. The standalone model is not a substitute for that test.
