# Pipeline default and window workspace

Implementation: `1ef3e9c28`, `a267336ed`, `da538c737`.
Registration/source used for all five campaigns: `0852571cf`, clean worktree.
Binary SHA256: `3cba155047e713e8ec30887f7d02f27d447a5af83a1c868390b2400c56fcf37d`.
See [delivery and registration](../../../../../docs/optimizer/pipeline-default-delivery.md).

Pipeline is the typed production default. Production Cascades is **retained**
as explicit quality/budgeted choices, never a silent error fallback.
This is not complete PipelineReady, optimality or DuckDB parity certification.

## Validation

- Final workspace: 7,022 passed, 85 ignored; strict workspace/all-target Clippy
  passed. Memory-runtime and fallible-vector-copy guards passed. Changed-file
  headers passed; full-tree header check retains 166 pre-existing findings.
- Window runtime: 21 tests passed, including fixed active/spare payload buffers,
  independent frame recomputation, NULL/FILTER/peers, varlen lifetime and errors.
- Physical tests: 158 passed before the added external-routine identity test;
  the final workspace run includes that extra test. Benchmark: 218 passed.
- Fresh-store, verifier-on, all-pipeline SQL regress: **164 passed, 21 failed**.
  The maintained whole-block audit is retained. Non-EXPLAIN differences are
  two owned fixture-path echoes, the absence of Memo-only metrics, and the
  newly authorized default/description in pg_settings. Other differences are
  plan snapshots. No other result differences were found. No expected file was
  updated, and raw regress is not relabeled green.
- The first attempt exposed missing EXTERNAL_PROJECT/TABLE typed identity;
  that real regression was fixed before the final run. A startup-race attempt
  failed to connect before running SQL; the replacement run has a separate path.

## Exploratory fresh-process screening

Two process blocks, one warmup, one normal measurement round per block, one
separate bounded Detail capture. Four threads, 2GB, binary protocol, normal
trace/verifier off and bounded compile receipt observer on. DuckDB 1.5.5 and
native hash are pinned in each original inputs manifest. Generator-declared
metadata makes this track non-qualifying for parity. Host background load was
present. These are descriptive medians in milliseconds, not a causal A/B or a
powered performance gate.

| Query | Compile | Paro C1 | DuckDB C1 | Paro warm | DuckDB warm |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | 9.560 | 283.757 | 149.257 | 198.824 | 134.283 |
| Q11 | 6.285 | 192.112 | 88.545 | 141.121 | 100.068 |
| Q51 | 3.048 | 2079.250 | 139.977 | 2120.892 | 133.469 |
| Q58 | 5.640 | 119.887 | 12.047 | 62.670 | 7.804 |
| Q74 | 5.466 | 110.532 | 95.081 | 67.347 | 71.435 |

All five queries pass complete result/type/bag/ORDER validation. Q58 now
executes its five-row SF1 result rather than failing output-name binding.
Q51 is directionally lower than historical ~2.6s, but this run has no matched
old-binary arm and cannot isolate workspace reuse from machine variation.
Compile medians use the two actual cold receipt observations, not diagnostics
or cached compile metadata from warm samples.

`validation.json` was generated through the maintained campaign, cell and
compile-document validators; all ten cells and five captures validate.
Original `q*-run/` packages preserve samples and identities once. Raw server
logs, binaries and data directories are not archived here. The regression
whole-block audit is in `regress-audit.json`.

Still unfinished: Q39 independent certification, whole-corpus confirmation,
the old spill reproducer, bounded execution profiling/cold-path attribution,
RF ablations and formal default-path non-inferiority. No global ProofComplete
claim follows from bounded pipeline planning.
