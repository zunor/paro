# U-BATCH: contract blocker, not a measured performance negative

The proposed whole-expression atomic batch conflicts with two existing observable
contracts even with frozen child inputs and unchanged cost comparisons:

1. After mandatory baseline, one optional credit and two new tuples: current
   path admits/prices/publishes the first tuple; atomic N=2 rejects both. The
   retained incumbent and synthesis count differ. `admit_optional_units` is
   atomic, not a partial-prefix reservation.
2. Publication can yield to a parent or quality-policy handoff before remaining
   tuples run. Whole-expression publication runs that tail first, changing
   synthesis count and visibility at the same stopping boundary. Purity of the
   arithmetic does not make publication unobservable.

The independent four-test finite oracle passes. New Rust budget counterexample
`optional_combination_prefix_is_not_whole_expression_atomic_admission` passes
on clean92b904a5 (one exact filtered test actually ran). Full optimizer run also
passes the existing recipe retry, yielded-child consumption, frontier-preview,
delta-combination, facts/grant/calibration identity, merge and RF-sensitive-child
tests. Full suite is **1213 passed/5 failed**, not green; see hygiene raw logs.

Source/standalone proof details are in CONTRACT-REVIEW.md. The original note's
"not executed" markers refer to its pre-test review; actual execution status
is the paragraph above and the archived `paro-ubatch-oracle.log` /
`paro-ubatch-rust.log` under `../hygiene/raw/unit/`.

**No batched search implementation or U performance A/B was run.** U≤12µs is
unverified, not rejected. It would be false to close the protocol hypothesis or
conclude "only reducing N remains" from this semantic counterexample. Existing
U≈20µs is a two-point residual slope, not a measured cost of the protocol code.

A decision is required before the requested atomic rewrite: allow resumable,
budget-prefix/publication-bounded microbatches (not one registration for the
whole expression), or deliberately change the partial-budget/publication
contract and validate it as a separate policy change. The latter violates this
round's unchanged-semantics boundary and was not implemented. No budget, cost,
frontier, scheduling or default policy changed in this round.
