# Regional response kernel: evidence and limits

This is an exploratory implementation comparison, not default promotion,
exhaustive optimality or DuckDB parity. Read [registration.md](registration.md).
The default remains `quality`; the pipeline reports `Incomplete`, and a
transition-budget fallback now explicitly reports `BudgetLimited`.

## Implementation

- `09fe0a3d5` shares connected-subgraph enumeration between the existing join
  planner and regional grain states. Legal ordinary inner regions include mixed
  comparisons with single-relation scalar operands, up to 12 atomic inputs.
  Outer/reduction joins, evaluation fences and imposed build contracts remain
  boundaries, not permission to reorder arbitrary joins.
- Native local response preparation uses the existing estimation equations and
  physical cost kernel, without constructing an owned child plan for each
  transition. Known-NDV equality cuts without new residual predicates use a
  borrowed lookup and defer output fact assembly until state admission.
- Source responses separately charge output-vector materialization. A legal RF
  can replace only that source component; access/decode, independent work and
  memory floors stay charged. Repeated demands are not multiplied as if they
  were independent. This is an explicit model intervention, not just a cache.
- `b07050f6b` exposes pipeline selection in the maintained execution diagnostic
  collector and SQL regression runner. It does not add a new timer/exporter.
- `1976ceb62` fixes a Filter input/output layout namespace error discovered by
  the new real Session tests in the quality reference. Projection identity must
  use the child's width, not the projected Filter output width.

The inner loop is **not yet fully compact**: unknown-NDV, residual and partial
aggregate transitions still complete output statistics; retained candidates
still carry layouts/bindings. Native inequality capability pricing does not yet
cover every owned-node physical gate. One winner per grain is a heuristic, not
a proof of dominance under every future continuation. No claims otherwise are
made by the enumeration oracle.

## First probe (before the Filter namespace correction)

All four pipeline and quality cells completed with exact typed/bag/ORDER result
validation. Three fresh blocks per cell, 4 threads, 2GB, verifier off, normal
trace-off compiler receipts, independent Detail. External VM/Go/indexing work
was observed throughout; retain all slow samples. Generator-declared metadata
also prevents an unqualified cross-engine parity claim.

Milliseconds, medians; these are separately collected cohorts, not an isolated
causal speedup or subtraction-based execution decomposition:

| Query | Previous pipeline compiler | Probe pipeline compiler | Quality compiler | Pipeline C1 / warm | Quality C1 / warm | DuckDB C1 in pipeline cell |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q04 | 11.432 | 9.365 | 22.323 | 244.463 / 172.866 | 255.683 / 145.274 | 144.412 |
| Q11 | 4.756 | 4.928 | 13.245 | 162.858 / 99.058 | 176.497 / 97.777 | 75.373 |
| Q74 | 4.235 | 4.984 | 13.309 | 118.044 / 75.536 | 135.251 / 74.684 | 95.176 |
| Q72 | Failed | 15.490 | 401.284 | 746.681 / 651.025 | 1106.883 / 705.428 | 30.148 |

Q04 Detail region time is 5.959ms before and 4.268ms in the probe. Its ordinary
99 and aggregate 51 transitions are unchanged; the probe prices 92 borrowed
cuts and completes 120 outputs. The <=1ms region target is not met. Q11/Q74 do
not demonstrate compile improvement. Structure identities differ across the
before/probe boundary, so this is not a same-plan representation-only result.
The broad build-side/aggregation execution quality gap has not been closed.

Q72 enters the expanded ordinary region (917 transitions, 404 borrowed cuts,
661 completed outputs, no region budget fallback). The previous normal cell
failed with `InternalError_: Block handle is None`. Preserve that failed cell;
do not replace it with a successful diagnostic or claim a paired speedup.
`q72-before-analyze.txt` independently shows a 16,425,000-row build and spill
replay (~10.5s diagnostic execution). The new successful plan is not a fix or
root-cause proof for that storage error.

Control binary: `218e1ee87229c7f5e62db8faab4b70988f75e2c4b7bda2d8972fee2f9b327e1d`.
First probe binary: `af47a767260c0db8ed137bd92e5c205618699aca944ad2438a2b624948f5576c`.
The first probe was collected at clean `b07050f6b`; each cell's `inputs.json`
owns its exact source/build/SQL/seed/configuration identities. The baseline
binary was overwritten by the initial rebuild rather than retained separately;
its original build attestation and report remain, but a contemporaneous binary
ABBA comparison cannot be claimed.

## Correctness and remaining migration barriers

The initial final workspace check via `make test` passed 7,007 tests, with 85
ignored; strict Clippy passed. Benchmark tests: 215 passed. Regression harness:
104 passed, one skipped. Small independent subset traversal and full native
response checks cover the retained equality/grain states. New Session tests
exercise mixed comparisons, non-equality orientation, an outer-join boundary,
duplicate multiplicities and an independent expected SUM.

Full SQL regression on the probe binary: quality 183 passed, two failures.
Both failures are imported Python fixture paths under the owned report root
instead of the historical `<repo>/regress/report` text; raw expected/actual
material is retained in `regress/`, not blessed. Pipeline full-suite execution
stops at explicitly unsupported write planning in INSERT/setup. Empty downstream
tables are not evidence of a bad SELECT result: this suite did not successfully
set up the pipeline run. Full pipeline regress and write coverage remain open.

No fixture was rewritten to hide a failure. Q39/Q58, the broad corpus gate,
runtime adaptive build orientation and the Q72 storage failure are not claimed
resolved by this slice. No historical worktree, data or recovery material was
removed. Runtime data/server logs are not archived here.
