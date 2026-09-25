# Join contracts and corpus-driven window execution

This is a staged implementation and exploratory performance delivery, **not**
default promotion, full correctness certification, exhaustive optimality or
DuckDB parity. No expected results, search budgets or default policy changed.

## Decisions and implementation

The corpus-priority recommendation was useful. Treating every Filter above a
join as an error was not: outer/reduction boundaries, multi-input expressions,
OR, projection namespaces and evaluation fences must remain explicit.

- `3172bba7c`: canonical inner-region comparisons share connectivity, pricing
  and reconstruction. Eligible equalities remain composite hash keys;
  non-equality comparisons remain join residuals, not a materialized Cartesian
  match stream followed by a Filter. Independent outer/NULL/result tests remain.
- `035a96a89`: native Memo reconstruction consumes predicates at their first
  complete support, including leaves. A DP point estimate is no longer published
  as an exact cardinality fact. The first quality Q72 OOM is retained below.
- `8c419d172`, `4b0b157f6`: bounded corpus-impact ranking, fixed wide-query
  metadata admission and cleanup when a DuckDB worker is interrupted before
  context-manager entry. Rankings retain missing/failed queries rather than
  assigning them zero runtime.
- `6832ffe7e`: append-only aggregate windows reuse one bound aggregate state.
  Evaluated frames must have a fixed lower bound and nondecreasing upper bound.
  Only delta rows update the state; finalization uses the existing observational
  ABI. Moving/shrinking frames retain independent recomputation. Tests cover
  FILTER/NULL, peers, chunk boundaries, exact update counts and error destruction.
- `7fa9642a5`: full regress exposed a latent delimiter substitution error:
  inequalities had been treated as column identities. Substitution now requires
  equalities covering each delimiter column exactly once. Existence
  decorrelation accepts the canonical join without an empty Filter; real
  LATERAL delimiter execution has typed capture/scan identity encoding.

These are generic contracts, not query IDs, rule-count witnesses, forced build
directions or a new optimization-policy lane. Registration and amendments live
in [the task record](../../../../../docs/optimizer/join-predicate-contract-task.md).

## Final normal measurements

Final production source: `7fa9642a5`; binary SHA256:
`6fc0d303e8821e86dc10f8d470f9d882d16d9d19b8aed5c3754d5c4e00fe765a`.
DuckDB 1.5.5, 4 threads, decimal 2GB, verifier off, trace off,
`PARO_COMPILE_WORK_EVIDENCE=1`, binary protocol, complete typed/bag/ORDER checks.
Source/build/seed/SQL/metadata and observer identities remain in each original
`inputs.json`; no source identity was relabeled after collection.

Final pipeline Q01–Q99 screening has two independent fresh blocks per query,
one warmup and one measurement round per block, and a separate Detail capture.
Compiler medians below use the cold normal receipts, not Detail time. Milliseconds:

| Query | Compiler | Paro C1 | DuckDB C1 | Paro warm | DuckDB warm |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | 9.410 | 256.572 | 140.906 | 160.216 | 121.339 |
| Q11 | 4.579 | 133.314 | 61.347 | 84.890 | 54.776 |
| Q74 | 4.006 | 113.698 | 87.653 | 64.178 | 74.869 |
| Q72 | 18.579 | 329.062 | 34.120 | 153.854 | 26.371 |
| Q51 | 2.434 | 2658.799 | 128.682 | 2627.971 | 123.720 |

The separately registered three-block Q51 pilot measured C1 **2581.519ms** and
warm **2536.467ms**, versus pre-window pipeline screening **8074.097ms** and
**8102.765ms**. All results matched the DuckDB oracle. Normal receipts retain
the same structural identity, selected fingerprint and class 0 across those
Q51 cohorts. Matching identities locate the same selected shape; they are not
an equivalence proof. The independent tests additionally establish linear input
consumption for append-only frames.

This is a large directional end-to-end improvement, **not a certified causal
speedup**: builds/cohorts were sequential, sampling differs, and VM/database/
background host load was present. Do not pool these samples or compare ratios
from unrelated DuckDB blocks. Neither warm non-inferiority nor parity was
pre-registered with sufficient power. No same-process cross-policy warm
alternation experiment was implemented or claimed.

## Coverage and priority

- Final pipeline TPC-DS: **97 passed; Q39 and Q58 failed**. Q39 retains its
  strict numeric bag mismatch; Q58 retains the ambiguous output-name binding.
  The final campaign is correctly `Incomplete`, not a successful corpus gate.
- Pre-window quality: also 97/99, on its own recorded binary. The initial
  pipeline coverage was split at a Q66 metadata-capacity failure; its union is
  screening evidence, not a complete accepted campaign. Final pipeline coverage
  reran all 99 on one binary with the corrected fixed quota.
- TPC-H: both final policies ran all 22 queries: **16 passed, 6 failed**
  (Q01/Q02/Q10/Q13/Q15/Q20). The failure set and reported digests agree with
  the retained [DuckDB fixture audit](../relation-convergence-v1/tpch/tpch-duckdb-fixture-audit-v2.json).
  This is not permission to bless static or floating expectations. Fixture setup
  explicitly used quality; pipeline writes have not thereby been certified.
- Final pipeline's 97 measured warm medians sum to 9277.110ms. The slowest five
  account for 61.7% of that sum. Ranking by excess warm milliseconds identifies
  Q51, Q67, Q78, Q47 and Q23 first. This is an equal-frequency prioritization
  proxy, not campaign elapsed time or a production workload distribution.

Q72's inspected plan now attaches d2 on the catalog side, consumes the composite
item/date join and its range residual, and emits 10,552 rows at the inventory
join before subsequent reduction to 2,024. Inventory still scans 11,745,000
rows. No claim of storage range-pruning completion is made. Existing per-key
membership/zone-map infrastructure exists; composite source-response costing
needs per-key/domain evidence, not first-key NDV masquerading as tuple NDV.
The old spill `Block handle is None` failure is not certified fixed by a plan
that no longer spills.

## Diagnostics and retained negative results

Each campaign contains the original bounded RunOutput manifests, attempts,
normal receipts and single capture references. Server logs, binaries, data
copies and raw event floods are excluded. The registered archive cap is 64MiB.

- `probe-*`: initial eight-query pipeline pilot. `quality-72-run` preserves
  the initial OOM; `quality-v2-72-run` preserves the corrected but still slow run.
- `corpus-pipeline-run`: old partial Q01–Q65 collection, Q39/Q58 errors, Q66
  publication failure. Its summary incorrectly remained `Running` while its
  manifest became `Incomplete`; the shared validator rejects that combination.
  Do not rewrite or certify this package. Its individual cells validate.
- `corpus-quality-run`: deliberately interrupted after five queries to avoid
  repeating the known Q66 capacity failure. `Running` is unfinished evidence,
  not success. `corpus-v2-*` are separate replacement runs, not overwritten retries.
- `window-51-run`, `window-corpus-pipeline-run`: final normal cohorts.
- `outlier-*-run`: one verifier-on typed COMPILE ANALYZE capture for the five
  largest excess contributors plus Q72. Capture/lifecycle validation succeeds,
  but operator profiles remain **Uncovered** in the maintained D6 collector.
  `status=ok` does not mean operator attribution exists. No diagnostic client
  time is used as C1 or execution-only time.
- `inspection/`: bounded human-readable SQL inspection, including the wrong
  correlated results and retained-control contrast. These are not a legacy
  profile exporter, machine-parsed D6 evidence or a quantitative timing gate.
- Initial TPC-H setup failed before executing queries: decimal 2GB was below
  the workload's 2GiB minimum. `tpch-v2` uses exactly 2,147,483,648 server bytes;
  the TPC-DS resource envelope was not changed.

See [validation](validation/validation.json) and the unmodified before/final
regress outputs. The full suite first exposed three new correlated result
errors; these were fixed, not blessed. Final regress: **182 passed, 3 failed**:
two pre-existing Python IMPORTS path expectations and one intended CTE mixed-
join EXPLAIN change. No expected file was changed. Global header checking still
reports 166 existing issues; this is not an all-static-checks-green claim.

## Next gates, not completed work

1. Close Q39/Q58 and the independently audited fixture contracts. Keep default
   promotion blocked; do not equate agreement between two Paro policies with
   an independent correctness oracle.
2. Continue Q51's window execution work: linear input consumption is now proven,
   but per-result/materialization overhead remains to be attributed and reduced
   through the aggregate/vector owner, not function-name dispatch.
3. Model composite RF responses per key/source and validate actual scan savings
   on Q72. For Q04/Q11, first run a controlled build-side ablation; no new
   runtime adaptive-side mechanism is justified solely by an estimated NDV.
4. Investigate Q67/Q78/Q47/Q23 by real operator work. Extend the existing typed
   execution record with bounded operator metrics; do not build another raw-log
   parser or infer causes from SQL feature names.
5. Finish pipeline writes, access-path and low-resource coverage and registered
   non-inferiority gates before switching defaults/removing the old solver.
   Compact remaining DP transfers only against the new attribution; no global
   search-budget increase, hard-coded join order or unreported timer narrowing.
