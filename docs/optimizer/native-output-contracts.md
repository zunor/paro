# Native rewrite output contracts

Native rules consume canonical Memo payloads, but publish an exact occurrence
interface. A canonical carrier layout is not permission to return additional
columns, and a new relation is not permission to copy its input's statistics.
These are separate contracts; repairing one by recomputing all of the other
can change unrelated physical selection.

## Publication and property ownership

- Restore Filter/Order occurrence projections from the recorded output
  `ColumnId`s using the same local layout adapter as the owned boundary.
  Join and TopN rewrites retain their existing shape-specific expansion and
  root-interface checks. Do not expand opaque group holes to reconstruct a
  convenient representative tree.
- TopN/late-payload publication propagates output demand separately from
  expression inputs. A scan must still read predicate columns, but its parent
  need not transport them after filtering. Shared demand rules preserve
  positional/full-row boundaries and exact group-hole interfaces.
- This projection-only operation retains row estimates and source evidence;
  it rederives layout-dependent unique keys. It does not compact Get bindings
  or create a second column namespace.
- Join preaggregation introduces a genuinely different relation. Its partial
  aggregate and changed join start without copied row/uniqueness facts, then
  use transactional native relation derivation before Memo publication. An
  input of 4,050 rows grouped into two keys must not price the partial output
  as another 4,050-row relation.
- Nullable uniqueness and aggregate argument nullability remain distinct.
  `COUNT(nullable_column)` cannot become `COUNT(*)` from observed non-null
  data, and a final grouping above a null-extending join is not removable
  merely because one input has a nullable unique constraint.

## Search-provider windows

A provider's legal window is a property of operand bindings, not whether a
projection happens to use the `All` enum variant.

Both borrowed native windows and owned/dependency inspection validate the
same scan/filter path bottom-up. Each predicate resolves against its input;
each output map is in range; the final projection resolves against the retained
bindings. Named scan bindings may survive narrowing or reordering. Positional
references are admitted only when their slot still names the same scan column.
Foreign, correlated, missing or mistyped bindings decline the specialization.

Thus restoring a precise Filter output does not silently disable full-text
TopK or filtered vector providers. Residual predicate, score identity,
capability, parameter and truncation checks still apply; this is not a forced
index choice or a weaker provider-equivalence test.

## Regression discipline

Review all transcript blocks, not only the first mismatch reported for each
file. A constant failure count can hide a newly failing case. Use an immutable
test binary because runtime-profile regression cases restart the server;
rebuilding its executable path during a run mixes sources. Use a fresh owned
database to avoid confusing hand-written reproducer rows with engine errors.

Plan transcripts retain columns, estimates, operators and resource contracts.
Only allocation-dependent logical ids use the existing alpha-renaming
normalizer; missing ids or different sharing relationships still fail. Typed
search tags and the established node/detail indentation replace obsolete
Debug labels and old formatting. Never normalize away a changed provider,
wide pre-fetch payload or missing preaggregation.

An initial attempted repair refreshed every TopN/late-payload statistic. It was
discarded: layout repair must not be coupled to reestimation of unaffected
relations. The subsequent layout-only repair exposed a separate provider
matcher defect (`projection_map.is_all()`); the binding proof above fixes it
instead of accepting lost search execution coverage as a new baseline.

### Validation (2026-09-23)

Starting source: `37ddf9b4`, clean `re-op`; implementation: `c8a0ddbd`.
Release binary SHA-256:
`b5df2d1e4a9f7ebb1c2bb3385fe5f2be3749704824a05e6e2a57b652dae6fbe4`.
The new Rust tests cover production preaggregation publication, native
occurrence restoration, demand versus predicate inputs, and provider binding
permutation/negative controls. No search budgets or stop policies changed.

- `RUST_MIN_STACK=33554432 cargo test --workspace --locked`: 6,928 passed,
  zero failed, 85 ignored; optimizer 1,389 passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `make -C regress unit`: 103 passed, one skipped.
- Immutable release binary, fresh owned data directory, 4 threads / 2GiB,
  FD limit 65,536, `runner.py --optimizer-verify on --verbose`: **185 passed,
  zero failed, zero skipped** after the reviewed transcript update.
- Before updating expected files: 168 passed / 17 mismatches. All 54 changed
  blocks in those 17 transcripts were EXPLAIN; every other transcript block
  was byte-identical. Changes were separately reviewed as rendering/typed
  tags, logical identity fields/schema, stable RF identity/resource output,
  estimates, or null-safe preaggregation. No SQL result baseline changed.
- Changed Rust hunks pass rustfmt; `git diff --check` passes. Repository-wide
  formatting/header debt remains outside this task (the header checker reports
  169 existing issues in untouched files); this is not an all-static pass.

No new Q11 performance campaign was run for this correctness change, so the
following historical attribution is not a performance acceptance claim.

## Next compile-cost work

The preceding [physical-core campaign](../../benchmark/evidence/optimizer-migration/20260923/physical-core-v1/README.md)
did not establish the requested 50% compiler reduction. Its final Q11 normal
median was 13.500ms; its separate Detail interval was 13.477ms. The latter
assigned 2.699ms to preparation, 1.422ms to finish, 1.359ms to subproblems,
1.188ms to recipes and only 0.229ms to the actual pricing kernel. These are
historical diagnostic observations, not fresh timings of this correction.

The next experiments should target real work, in this order:

1. **Demand-driven preparation.** `Optimizer::optimize` currently forks every
   canonical query for DISTINCT feasibility, even without an eligible
   aggregate. Put a read-only applicability proof in the decomposition owner;
   fork only eligible inputs. Preserve the mandatory low-grant path for
   DISTINCT state. Extend this into construction-time normalization/effect
   summaries only after counting which full passes actually visit unchanged
   nodes. Do not remove dependent passes based on operator names alone.
2. **Consume-and-seal finalization.** Normal search currently collects
   per-group/frontier/source-payload profiles, clones rule maps and publishes
   dynamically named diagnostics. Keep mandatory safety/coverage/stop receipts
   and bounded summary counters, but construct detailed matrices only for
   explicit capture. Use the existing typed compile owner, not another export
   path. Measure collection, verification, extraction and destruction
   separately; moving any of them to admission is not a saving. Existing
   session inspection consumers must receive a deliberately specified summary,
   not accidentally empty data.
3. **Exact task completion before more caches.** Subproblem/recipe setup is
   larger than numerical pricing. Identify repeated setup after unchanged
   responses and charge it to the exact goal/read revision. Preserve the
   distinction between cached prices and active response membership learned
   in the physical-core task. Do not retain incomplete frontiers as closed or
   replace parent-dependent response alternatives with one scalar winner.

Local reference revisions inspected for these directions:

- CockroachDB `8812064a015`, `pkg/sql/opt/norm/factory.go`: normalization and
  interning on construction; `pkg/sql/opt/xform/optimizer.go`: optimization
  state per group/required property and completed-member tracking.
- DuckDB `d8cdaa33fd`, `src/optimizer/optimizer.cpp`: explicit ordered passes,
  including repeated column-lifetime work. It is not evidence that all
  normalization can be fused into a single pass.
- StarRocks `1aba139bbf`, `OptimizeGroupTask.java`: property-context reuse and
  bound checks before task expansion. Its scalar-bound assumptions are not
  interchangeable with Paro's RF/source response, sharing and memory overlap.

These are architectural references, not cross-engine latency measurements.
Before claiming a speedup, register independent normal/Detail Q04/Q11/Q74
cells, validate full typed results, and associate exact receipts. Keep search
budgets, verification, grants and stop policy fixed. A changed selected plan is
a separate intervention. No single current bucket supports a promised 6ms
saving, and `QualityPolicySatisfied + SearchIncomplete` is not `ProofComplete`.
