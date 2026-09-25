---
name: paro-optimizer
description: Design, refactor and diagnose Paro's staged optimizer, using EXPLAIN COMPILE for planning and EXPLAIN ANALYZE for execution. Use for optimizer changes and plan-quality investigations.
---

# Paro optimizer

Start with the selected checkout's HEAD/status and [architecture](../../../crates/optimizer/readme.md).
Preserve unrelated work. Keep one production planner, not alternative policies.

## Place the change with its owner

- `rewrite::program`: ordered semantic replacements, without cost alternatives.
- `estimate::annotate`: shared column/relation/selectivity kernels; distinguish
  expected estimates, proven bounds and unknowns.
- `region::plan`: interacting join/aggregate decisions, bounded enumeration.
  Transitions consume compact summaries, not cloned plans or full physical selection.
- `physical::choose / lower`: local algorithms, access paths, RF and committed
  physical construction. Cost assumptions must match emitted keys/residuals.
- `paro-planner`: binding and shared logical/physical contracts. Execution
  consumes plans, not optimizer internals. Runtime adaptation belongs in execution
  and must respect memory, spill, cancellation and output contracts.

One concept has one owner. Keep production kernels and their tests; remove
uncalled alternative algorithms instead of retaining them as test-only “oracles”.

## Diagnose before changing a decision

1. Check estimated versus actual rows at the first divergence, including CTE
   boundaries, complete keys, NULL semantics and predicate activation. More
   enumeration does not repair wrong estimates.
2. Use `EXPLAIN (COMPILE, DETAIL, FORMAT JSON)` for stage work and bounded
   regional decisions; use `EXPLAIN ANALYZE` for actual operator behavior.
   Read [compile diagnostics](../../../docs/optimizer/compile-diagnostics.md)
   and the actual typed schema, not old report names.
3. For a costly choice, inspect both alternatives' rows, widths, work and
   feasibility. A scoped forced-choice experiment can isolate its effect;
   don't ship query-specific join/build hints or fit constants to one query.
4. Prioritize corpus excess-time/outlier rankings, not only Q04/Q11/Q74.
   A measured local decision can justify runtime adaptation; “unknown” alone
   does not make an unimplemented adaptive path safe.
5. Measure first-execution phases directly. Differences of cohort medians
   give scale, not a causal breakdown.

`Planned` / `PlannedWithFallback` describe legal artifact production, not
global optimality. Fingerprints associate plans; they do not prove SQL equivalence.
Never pair normal timings with an unrelated diagnostic plan.

## Validate proportionally

- Mechanical refactors: preserve public contracts, typed identity and EXPLAIN
  body; for broad planner changes compare TPC-H 22 / TPC-DS 99 before/after,
  full typed results, and SQL regress. Explain expected differences explicitly.
- Plan-changing work: counterexamples first (NULLs, duplicates, evaluation
  errors, outer/recursive boundaries), then real-entry tests and corpus
  results; inspect excess-time rankings for new outliers.
- Use [paro-benchmark](../paro-benchmark/SKILL.md) for ordinary performance
  exploration. Formal non-inferiority/parity uses paro-evidence, not every edit.
  No failed semantic comparison may be hidden by snapshot regeneration.

For source comparisons, reuse the agreed small worktree and one shared Cargo
target sequentially; save binary/source identities before rebuilding. Do not
create large targets/data copies automatically. Reuse immutable, relocatable
seeds. Normal compile timing needs the checkout's bounded compile-work observer;
verify an actual receipt before collecting more samples. Missing compile data
is uncovered, not zero, and Detail time is not a normal compile sample.
