# Finite regional pipeline pilot — not promoted

The vertical slice reaches real compilation, admission and execution using
`optimizer_search_policy=regional`. It has an ordered logical program and a
closed physical catalog. It does not remove the shared physical solver or
prove global optimality. The SQL default remains `quality` because the pilot
does not establish a performance/plan-quality improvement.

## Evidence identity and limits

Collected before analysis under [registration.md](registration.md). Each of
the six `*-run` directories is an unmodified maintained RunOutput with its
input/build/source manifest, accepted attempts, normal cell receipts, one
separate diagnostic capture, and campaign summary. All six validate with
`harness.receipt_contract`; normal samples require Verified receipts, whereas
non-executing diagnostic captures are validated as such, not passed off as
executed samples. No free-text server log or duplicated trace is archived here.

- Base commit: `e65146fa4e11055d001ab7559cd09d03578d7e17`, task changes uncommitted
  at measurement. Dirty source SHA:
  `348ab024f72759305433a95a86f3e9478b9e99c46169280398a89533c3155c54`.
- Measured release binary SHA:
  `5eb99370f923701260a7a17ff82f58610eb54e6e8bcaf5ca0eee96358977d10b`.
- Same binary/settings/seed between Paro arms except SQL planning policy;
  four workers, 2GB, binary PgWire, verification off, normal trace-off,
  `PARO_COMPILE_WORK_EVIDENCE=1`, 30s optional deadline, 60s statement timeout.
- Two independent fresh processes per query/arm; two ABBA warm rounds per
  process; one diagnostic process per cell. All samples retained. Queries ran
  Q11, Q04, Q74, with regional followed by quality for each query. Source arms
  were not interleaved at each individual sample; ambient VM/other workload
  persisted. These are exploratory measurements, not powered causal estimates.
- DuckDB 1.5.5, exact native/extension/input identities in `inputs.json`.
  Metadata track is generator-declared; no cross-engine parity certification.
- Q11/Q04/Q74 complete typed results, multiplicities and required ordering
  passed in both policies. Every normal occurrence-zero target was a cache
  miss. Subsequent receipts represent actual warm execution, not EXPLAIN time.

## Normal results (ms)

Compiler lists contain **both fresh observations**, including the slow Q04
sample; C1 and warm columns are the maintained collector's medians.

| Query | quality compiler samples | regional compiler samples | quality / regional C1 | quality / regional warm |
| --- | --- | --- | --- | --- |
| Q04 | 18.566, 19.426 | 44.275, 113.889 | 234.601 / 369.377 | 147.836 / 157.951 |
| Q11 | 12.636, 15.146 | 25.506, 28.113 | 210.521 / 161.535 | 102.459 / 92.938 |
| Q74 | 34.963, 37.982 | 24.753, 29.383 | 142.932 / 230.368 | 62.480 / 160.634 |

Q11's lower C1 with higher compiler time is not evidence of compiler speedup.
Q74's lower compiler time with much slower execution is not an acceptable
tradeoff. Both Q74 arms actually admitted class 2, four tasks and a 2GB ceiling;
neither receipt reports a fallback. Do not attribute its execution difference
to a changed worker grant or to a cost-model defect without selected-plan
analysis. No C1/warm subtraction is used to claim a phase decomposition.

## Separate diagnostic evidence

| Query | quality → regional groups / logical / physical | cost compositions |
| --- | --- | --- |
| Q04 | 69/77/121 → 157/206/186 | 543 → 1650 |
| Q11 | 53/61/105 → 105/140/120 | 380 → 974 |
| Q74 | 95/136/217 → 101/138/124 | 1052 → 956 |

Regional reports `Incomplete`, with an explicit finite-program scope
obligation, not `ProofComplete`. Quality reports `QualityPolicySatisfied`,
also not exhaustive optimality. Diagnostic counts describe their own captured
plans; no unmatched diagnostic time replaces a normal compiler observation.

The finite program removes logical reactivation but still prepares alternatives
that the quality-driven path can avoid, and prices the catalog eagerly for
all grants. These facts explain why smaller scheduling machinery does not
guarantee less work. Attribution between candidate-domain expansion, phase
ordering, contextual costing and eager portfolio work remains to be isolated.

## Earlier probes retained as negative results

Temporary raw pilot bundles remain at their explicitly owned paths:

- `/private/tmp/paro-regional-v1.sHEx2o`: all passes retained alternatives;
  Q11 normal compiler 43.659/48.078ms versus 12.632/12.847ms. Rejected: it
  recreated the normalization powerset.
- `/private/tmp/paro-regional-v2.OjCy1z`: stopped before a valid sample when
  eager grant pricing was incorrectly labelled as a lazy portfolio. The
  verifier was not relaxed; the production contract and test were corrected.
- `/private/tmp/paro-regional-v3.UUqP9H`: normalized domain wrappers prevented
  aggregate-region matching (no deferral attempt in Q11). Rejected despite
  lower planning cost than v1; Q11 warm execution was about 184ms.
- `/private/tmp/paro-regional-v4.p2sIPP`: source of this archived pilot. It
  establishes aggregate alternatives before domain transport, restoring those
  legal opportunities without retaining all pre-normal representations.

These exploratory iterations are not pooled or used as a confirmation cohort.

## Validation

- Workspace Rust tests: 6,973 passed before the final additional cancellation
  test. Final regional tests: 10 passed, including that cancellation case.
- Strict workspace/all-target Clippy: passed. Final workspace check: passed.
- Benchmark tests: 229 passed (one existing pytest-return warning).
- Regression harness tests: 104 passed, one skipped.
- Actual regional SQL regress, fresh data and verifier on: 180 passed / 5
  mismatches. All blocks in the five `.actual` files were re-compared with the
  maintained comparator: four EXPLAIN differences and one settings row only.
  No SQL result rows differed. This is not an all-green regression gate.
- Same-binary quality SQL regress, separate fresh data: 184 passed / 1
  mismatch, the updated optimizer-policy description in settings.
- The four regional EXPLAIN differences are `agg_join_subsumption` (RF task
  supply), `cte_optimizer` (CTE identity), `cte_partitioned_materialization`
  (different legal producer shape), and `explain_basic` (TopN vs sort/limit).
  No expected file was updated or blessed.
- Initial regression outputs under nonstandard fixture paths produced two
  transcript path mismatches. A control run reusing the already-populated
  database also exposed a non-idempotent prepared-cursor fixture. These are
  retained locally, not counted as valid fresh-control comparisons. Final
  runs used independent fresh directories and the normal `regress/report`
  suffix so existing path normalization applied without widening comparison.
- Repository-wide header check reports 169 existing issues, none in a changed
  task file. New Rust files have the required headers. It is not marked passed.
- `git diff --check`: passed. No budget, verification, runtime algorithm,
  calibration or baseline change was made to obtain these results.

Local validation logs and actual outputs:
`/private/tmp/paro-regional-regress.wkIiuP/`. Owned test servers were stopped.

## Decision

Keep the slice explicitly selectable for continued migration, not the default.
Next work must introduce maximal-region ownership and a closed, contextual
physical interface, and determine the Q74 candidate-quality loss. Adding more
caches or simply switching the default would not close those gaps. Promotion
requires broad result validation, justified plan-text updates, plan-quality
non-regression and a new controlled performance campaign. No parity or compiler
target is certified by this report.
