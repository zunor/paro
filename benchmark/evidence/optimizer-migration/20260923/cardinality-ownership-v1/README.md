# Cardinality ownership recovery

Implementation: `bbf9c699`; independent receipt validation: `2e4ef0a6`.
Registration: [cardinality-ownership-registration.md](../cardinality-ownership-registration.md).
Initial clean collection source: `1c470f44`; release binary SHA-256
`fa61fd575bed042b1087130a4673fd4e22ddf0e239da5cf0b80ff1a193bcd716`.

## Design and causal boundary

Equivalent alternatives are not independent statistical observations. The
current staging allowlist promoted six transformation classes to
ConstraintRefined; insertion then merged those estimates into group facts.
This made different equivalent shapes change costing evidence and scheduling.

The migration removes the rule-name promotion. Roots inherit their target
group's estimate; new relations still derive estimates. The existing Memo fact
owner now distinguishes equivalent publication from explicit statistics
refresh. An unchanged fact set cannot vote on cardinality; stronger complete
facts may refine it; incomparable facts and true group merges retain the
existing uncertainty merge. Hard bounds, column domains, row-preserving
dependencies, invalidation and rollback remain live. No fingerprint elects
the winning estimate. This does not claim to fix all estimation errors or
calibrate the cost model.

No change to quality certification, grant coverage, cancellation, time/work
budgets or mandatory-to-optional implementation coverage. All observations
remain QualityPolicySatisfied + SearchIncomplete, not ProofComplete.

Two rejected interventions matter:

- Restoring only root inheritance (without the fact-owner boundary) enlarged
  search to 595 groups / 1,123 logical expressions and about 839 ms C1. That
  partial intervention is not shipped as an alternative mode.
- Yielding physical drain to queued logical quality work produced about
  2,695 ms C1 and was completely reverted.

Historical `d3097038` plus the correct mandatory-to-optional coverage repair
still compiled Q11 in 11.955 / 12.455 ms (415 syntheses). Therefore that safety
repair is **not** the dominant explanation for the earlier 70 ms regression.
The historical collector is a separate exploratory bridge without current
typed identities; it is not a cross-version equivalence or parity proof.

## Initial clean-source results

Normal, fresh-process, cache-miss compiler receipts; not diagnostic timings.
P90 uses the maintained collector's percentile convention. All samples remain
in their RunOutput cells, including slow values.

| Query / verifier | Blocks | Compiler median / P90 (ms) | Syntheses | C1 / warm medians (ms) |
| --- | ---: | ---: | ---: | ---: |
| Q04 / on | 2 | 20.002 / 20.036 | 810 | 240.191 / 175.134 |
| Q74 / on | 2 | 64.001 / 65.862 | 1,065 | 157.253 / 59.260 |
| Q11 / on | 5 | 14.006 / 15.089 | 420 | 182.723 / 103.582 |
| Q11 / off | 5 | 12.420 / 14.287 | 420 | 129.043 / 76.327 |

The final six seconds of the Q11/off campaign overlapped an external Go build.
It is not isolated performance evidence; SQL and identity validation still
hold. The registration amendment allows one additional same-code cell after
that workload finishes. Do not pool cohorts or remove its 14.287 ms sample.
The on/off sequence is not a causal experiment in verifier overhead. There
is no <=12.000 ms, tail-latency, warm non-inferiority or parity certification.

Q11's five off samples are 14.287, 12.201, 12.320, 12.466, 12.420 ms; the five
on samples are 14.453, 15.089, 13.189, 13.273, 14.006 ms. The earlier current
baseline had 1,467 syntheses / 121 groups; the candidate has 420 syntheses /
53 groups / 61 logical expressions. A separate on diagnostic sees complete
same-candidate join/aggregate/predicate evidence at 10.646 ms, with no missing
quality facts. This diagnostic timestamp is not the normal compiler result.

## Registered post-interference cell

Source `c41441db`, clean `re-op`; the binary SHA-256 is unchanged. Switching
back to re-op caused Cargo to rebuild identical sources. The collector was
paused **before any sampling** while that build and the other task's Go
build/lint finished; build duration is not a compiler sample. No known
build/lint process was running immediately before resuming or after the cell
finished. The user paused the other builds; this task did not signal them.

`quiet-q11-off-run` contains the registered five fresh blocks, not a selected
subset or a pool with the prior campaign:

- Compiler samples: **12.626, 12.694, 14.034, 12.583, 13.965 ms**.
  Median **12.694 ms**, P90 **14.034 ms**.
- Optimizer median **11.835 ms**. It is not substituted for the compiler
  boundary to manufacture a <=12 ms result.
- All five perform 420 cost syntheses; all results (90 rows), types, bag/order,
  cold misses, receipts and the maintained campaign validators pass.
- C1 median **137.513 ms**, warm **79.324 ms**; DuckDB C1 **54.434 ms**.
  There is no first-statement parity or powered warm/tail certification.
- QualityPolicySatisfied + SearchIncomplete; no ProofComplete.

The historical approximately-12-ms scale is restored with current ownership,
identity and correctness contracts. A strict compile <=12.000 ms target is
**not** achieved. These are finite host observations, not a latency guarantee.
Both earlier cohorts and all of their slower samples remain archived.

## Validation and remaining failures

- `make test`: 6,906 passed, zero failed, 85 ignored; optimizer 1,369 passed.
- Strict workspace Clippy passed. `make -C benchmark test`: 206 passed.
- Release build passed; all Q04/Q11/Q74 samples passed complete typed result,
  bag and required-order checks (6 / 90 / 92 rows respectively).
- Maintained campaign/receipt/document validators passed all five archived
  runs. Diagnostic EXPLAIN is NotExecuted and has no execution receipt; this
  is not silently relabeled a Verified normal observation.
- High-FD, verifier-on SQL regress: 167 passed / 18 failed. The failure-file
  set and **all 18 actual outputs are byte-identical** to the pre-migration
  `final-regress` control. No expected files changed. This is not a green SQL
  baseline gate.
- Memory/vector API guards and calibration-artifact check passed. Repository
  `cargo fmt --all -- --check` and the header guard still report pre-existing
  violations outside this change's formatting/header scope; no all-static
  pass is claimed and no unrelated bulk formatting was committed.

Validation logs and the matching regress transcripts remain at
`/private/tmp/paro-chain-latency-recovery-20260923/`. The archived run directories
are bounded collector outputs with one capture per cell, not raw event/log
archives. The historical worktree, data seeds and other tasks are untouched.
