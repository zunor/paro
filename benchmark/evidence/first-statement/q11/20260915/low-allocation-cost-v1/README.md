# Q11 low-allocation physical cost synthesis v1

Date: 2026-09-15

This archive is the independent control/probe for task 2 of the compiler-cost
round. It measures avoiding duplicate child-reference materialization in the
physical combination hot loop after task 1. It keeps the candidate enumeration,
frontier policy, search budget, stop policy, cost model, handoff policy and
execution envelope unchanged.

## Scope and source identities

| arm | source | binary SHA-256 | role |
| --- | --- | --- | --- |
| control | `29adcd9f76075feb797979ca5c8e063c0d79b46b` | `37652c0d400d8d7e70294dea4218d9479e23c98b74745e401237026bda985e28` | resident identity only |
| probe | `cb366e58dddfe463bd2caec7bbca7bdcbbef6139` | `c971e36d892cbb0ac3e65ec59b07f66febdf4bca97ff0b26f95f1e3b63c03d27` | lazy child-reference materialization |

Both arms were built in clean release worktrees. The harness-recorded working-tree
identity was `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
The original SQL is [11.sql](11.sql). Common input identities were data seed
`d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`, DuckDB
`568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`, source CSV
`4c73cc53e1567cad44341276a09589c209919e1002a7a59fbbedbf9d8e7532f3`, and query
corpus `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`.
The four harness file hashes are in the raw reports and are identical between arms.
Both runs used `PARO_QUALITY_POLICY_HANDOFF=1`, 4 threads, 2 GiB, private fresh
data copies, cache miss, normal trace-off, one warmup, three rounds per process,
five serial fresh-process blocks and one separate diagnostic block.

## Normal Q11 results

All 30 measured rows per arm passed the complete typed schema, value, multiset
and order contract. Five blocks are directional pilot evidence, not the formal
36-block power campaign.

| arm | Paro C1 median / p95 (ms) | DuckDB C1 median / p95 (ms) | C1 ratio (95% CI) | Paro warm median / p95 (ms) | DuckDB warm median / p95 (ms) | warm ratio (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| resident identity | 167.072 / 168.390 | 109.134 / 111.524 | 1.523087 [1.512224, 1.534028] | 69.663 / 79.019 | 106.609 / 109.766 | 0.659748 [0.651700, 0.674622] |
| low-allocation cost | 168.198 / 169.211 | 108.406 / 109.598 | 1.544692 [1.533143, 1.555571] | 69.129 / 71.947 | 106.559 / 111.369 | 0.649747 [0.644325, 0.654643] |

The probe's C1 median is +1.125 ms versus its direct control and its warm
median is -0.535 ms. These differences are within a small pilot and do not
establish a repeatable end-to-end gain. The normal compiler-work side channel
was not enabled, so no C1 stage subtraction is used.

## Implementation and attribution

`cb366e58` changes `CostedChildCombination` to retain the exact immutable
`CandidateId` tuple as the semantic key. The recipe's ordered child goals are
used to resolve a `ChildWinnerRef` only while the hot synthesis loop needs the
child's immutable cost/source-work summary, or at a publication/diagnostic
boundary that truly needs the full reference. Winner construction still
materializes exact child references; stale/invalid membership continues to be
checked by CandidateId, not frontier ordinal.

This removes one per-combination `Box<[ChildWinnerRef]>` and avoids rebuilding
the same reference objects before candidate comparison. It does not remove a
candidate, change the frontier, or alter the number of cost syntheses. The
control and probe diagnostics are identical on the relevant search counters:
241/365/524 memo groups/logical/physical, 390/379/124 bindings/apply/inserted,
2061 syntheses, 149 recomputes, 1285 published candidates, and the same
implementation/subproblem/registry counts. The final winner plan is also the
same as task 1; the task2 change therefore did not create an additional plan
drift.

The five-block diagnostic reports approximately 73.333 ms optimizer and
73.415 ms compiler-return elapsed for the probe versus 74.001 ms and 74.078 ms
for control. Freeze/handoff were approximately 2.292/1.114 ms versus
2.314/1.135 ms. The reports did not enable `PARO_COMPILE_WORK_EVIDENCE`, so
allocation counters and compile-work fields are absent. The small C1 change is
not attributable as a proven wall-time benefit; the primary result is the
allocation contract and its fixed-work identity preservation.

## Verification

- Engine focused suite: 113 passed, including exact child-choice reconstruction
  after pause/resume.
- `cargo check --locked -p paro-optimizer` passed in the clean task2 tree.
- Full clean optimizer suite after temporary stale-fixture adaptation: 1322
  passed, two known undeclared-grant fixture failures, 0 ignored failures
  attributable to this change; no failure was blessed.
- Benchmark unit suite under `benchmark/.venv`: 134 passed.
- SQL regress on the clean task2 tree with `ulimit -n 65536`: 153 passed, 31
  failed, 0 skipped. No FD exhaustion occurred; expected-plan/output-format and
  existing grant/identity failures remain explicitly unblessed. The archived
  `sql-regress-report.txt.gz`, `sql-regress-error.txt.gz` and `sql-regress.log.gz`
  contain the complete run.

## Completion state

The low-allocation representation is implemented and preserves exact child
choices, invalidation and publication semantics. It has not demonstrated the
requested compiler <=30 ms or a repeatable C1 improvement in this pilot; no
search work was silently removed. The optimizer remains
`QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`. Formal C1/W
parity and warm non-inferiority are still open.

Raw reports and logs are compressed beside this file and checksummed by
[SHA256SUMS](SHA256SUMS).
