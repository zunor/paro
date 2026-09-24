# Direct pipeline vertical slice

## Outcome

The three implementation steps are present: preserve Q74's valuable decisions,
plan a committed relation tree outside Memo, and test semantic/resource
boundaries. The six-cell replacement matrix passed complete result, type,
multiset and ORDER checks and exact normal receipt association. Default policy
is unchanged. This slice is **not ready for default promotion**: Q04 execution
regressed and broader query/resource support is not certified.

Same release binary: `4bfc7a52f36013e86f5be7fc06dc1a1d9923b4fae12a707edb62040305432e7a`.
Clean source: `d737ce8d1f0f141663ac9670d2373c42d4fe7d03`.
DuckDB: 1.5.5. Full source, SQL, immutable seed, native extension, resource and
timer identities are in each run's `inputs.json`. See [registration](registration.md).

Three fresh process blocks per cell, four warm samples per block, separate
Detail process. Background VM/OS load and policy cohorts run sequentially;
these are exploratory observations, **NotCertified**, not a parity or causal
speedup gate. No slow samples were removed.

## Normal results (milliseconds)

| Query | Policy | Compiler median | C1 median | DuckDB C1 | Warm median | DuckDB warm |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | quality | 18.388 | 240.818 | 146.077 | 135.696 | 113.917 |
| Q04 | pipeline | 7.032 | 264.943 | 136.735 | 181.569 | 120.369 |
| Q11 | quality | 12.801 | 145.610 | 69.131 | 83.570 | 55.930 |
| Q11 | pipeline | 4.342 | 170.635 | 80.164 | 98.426 | 66.343 |
| Q74 | quality | 35.219 | 129.376 | 81.602 | 57.936 | 69.198 |
| Q74 | pipeline | 3.570 | 109.038 | 85.902 | 64.354 | 69.201 |

Cold compiler samples, in collection order (microseconds):

| Query | quality | pipeline |
| --- | --- | --- |
| Q04 | 19923, 18388, 18355 | 7213, 7032, 6898 |
| Q11 | 12986, 12801, 12522 | 4043, 4342, 4831 |
| Q74 | 35938, 35219, 33201 | 3555, 3572, 3570 |

Compiler is the normal SELECT receipt's `compiler_elapsed_us`, not Detail
time. All 18 cold receipts are verified cache misses with non-null work.
All 72 warm receipts are verified. Candidate/plan changes are intentional;
this is not an equal-search-work microbenchmark. Both policies remain
incomplete relative to global search; pipeline does not claim quality-policy
certification or ProofComplete.

The substrate change reduced observed compiler medians substantially. It did
not solve end-to-end quality: Q04/Q11 C1 did not improve in this cohort. Q11's
DuckDB timings also drift between cohorts, so its entire difference cannot be
attributed to the planner. Q74 retains a much better execution shape than the
old regional-Memo experiment, but does not prove warm non-inferiority here.

## Plan inspection and next boundary

Separate read-only EXPLAIN inspection on this binary found:

- Q74: both branches retain date-domain pushdown, narrow-key partial aggregate,
  customer join and original-group final merge. This is not merely a count of
  aggregate/RF operators. Pipeline adds RF at customer joins; quality does not.
- Q04/Q11: those producer transformations remain, but build sides differ.
  Quality builds narrow partial-aggregate results for the customer join;
  pipeline builds the wide customer relation in Q04's store/catalog branches
  and Q11's store branch. Row counts alone do not account for payload cost.
- Q04 consumer region: quality introduces the catalog second-year reference
  and applies one CASE ratio filter before the last web join. Pipeline joins
  the catalog second-year reference last, leaving both ratio predicates above
  the full six-reference join. CTE statistics and resulting join order differ.
- Both policies admitted four tasks and a 2GB ceiling. Q04's admitted working
  set differs (quality 344,207,416 bytes; pipeline 375,085,472); this is not a
  controlled single-build-side experiment.

These are candidate explanations, not a causal attribution of all warm time.
The next focused work is bounded regional costing of predicate activation and
build payload/phase costs, with exact-plan ablations. Do not return to global
Memo scheduling or encode a Q04-specific join order. Repeated stage-end facts
and subtree costing remain visible compiler costs for later reduction.

Pipeline Detail reports zero Memo groups/expressions. The selected logical
occurrences are 53/36/36 and final-pass local implementations considered are
73/49/49 for Q04/Q11/Q74. These counts exclude temporary aggregate-comparison
subtrees and join DP states; do not compare them to all Cascades cost syntheses
as if the scopes matched. The legacy fine-grained work partition does not yet
classify the new stages; zero Memo buckets do not mean zero optimizer work.

## Validation

- `RUST_MIN_STACK=33554432 cargo test --workspace --locked -q` passed on the
  final source after identity/admission fixes; existing ignored tests remain
  ignored. Workspace check also passed for the slice.
- Strict workspace Clippy and release build passed after both fixes.
- Optimizer real-entry test covers eight shapes, one grant, zero Memo groups,
  and matching portfolio/receipt expected class.
- Session real-execution tests cover ten query shapes and forced external CTE
  execution; complete rows/types/names match quality, plus independent expected
  values for shared dimension labels and aggregate counts.
- Three direct resource tests reject floor clamping, preserve spill capability
  and keep runtime-capped completion distinct. Typed table-function identity
  test checks payload changes and rejects opaque bind data.
- Benchmark tests: 229 passed (one existing pytest return-value warning).
- Memory runtime and fallible vector-copy guards passed. Full header check
  retains 169 pre-existing issue records in unchanged files; all task-modified
  source headers pass. This is not a claim that the complete static gate is
  green.
- Full default-path SQL regress: 182 passed, 3 textual failures, no expected
  updates. `transaction_settings_savepoint` retains the pre-existing policy
  description mismatch. Two Python UDF fixtures differ only because this run
  used an explicit owned report directory instead of the expected default
  fixture path (`fixed_width_fast_path`, `imported_helper`). This is not a
  full pipeline corpus gate.

## Evidence ownership and negative results

`runs/b-*-run/` is the valid same-binary matrix. Each directory is the maintained
RunOutput package, with manifest, inputs, accepted normal/diagnostic attempts,
typed receipts and a single bounded compile capture. The collector and
`validate_benchmark_payload` / `validate_campaign_summary` validated every cell
before the next started. Total retained runs are approximately 2.2MiB, with no
free-form server logs or event flood.

Preserved interrupted cohorts:

- Unprefixed: pipeline Q04 stopped at metadata table-function identity, before
  target timing. Its completed quality control is not used in the matrix.
- `a-`: Q04/Q11 results and timing survived, but pipeline receipt association
  is Uncovered due to the missing expected-class declaration. No compiler or
  joined diagnostic claim is made from these cells. Q11 had already started
  before the Q04 receipt check; Amendment B requires per-cell checks.

Neither issue was hidden by changing the validator, bypassing metadata checks,
inventing an identity, dropping timings, or updating result baselines.
