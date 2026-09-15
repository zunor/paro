# Goal-scoped physical dependencies v1

Date: 2026-09-16
Scope: optimizer physical-subproblem read, reverse-dependency, version, and
notification contracts. No execution-layer, budget, frontier-width, or stop-policy
change was made.

## Question and implementation

The pre-change path recorded physical dependencies as `(child group, child goal)`
but captured a group-wide physical frontier revision and indexed reverse parents only
by child group. A publication for goal B could therefore invalidate a parent that had
only read goal A.

The probe changes the existing Memo/TaskRegistry contract as follows:

- each group has a broad implementation revision and a per-`OptimizationGoal`
  frontier revision;
- `PatternRead` carries the exact physical goal, implementation revision, and
  goal-specific frontier revision;
- `physical_read_set()` reads the exact child goal used by each recipe;
- reverse physical parents are keyed by `(child group, child goal)` and retain the
  parent goal, physical expression, and recipe identity;
- implementation-domain changes still invalidate every affected observed goal,
  while a winner/frontier change invalidates only the exact goal;
- completion changes are reported separately from candidate-cost/frontier changes;
- merge, rollback, recanonicalization, cancellation, and recovery preserve the
  exact goal identity and conservatively invalidate affected reads.

The implementation reuses the existing `ReadSet`, `TaskRegistry`, `CandidateId`,
frontier and completion machinery. It does not select a single winner, discard
non-selected candidates, or use a goal-specific frontier revision as a proof of
search completeness.

The production-path regression test
`physical_read_of_one_goal_is_not_invalidated_by_another_goal_publication` registers
a parent recipe through `admit_candidate`, optimizes two child goals, captures the
real physical read set, publishes goal B, checks that the goal-A read remains current,
then exercises the production notification and goal-A recovery path. A related goal-A
publication does dirty and re-open the parent. The test also checks that two physical
goals retain separate read cursors.

## Reproduction and identity

Control and probe were built in separate clean worktrees from the same pre-change
source and probe commit. The benchmark reports are JSON payloads retained with the
historical `.json.gz` filename suffix used by the harness.

| item | control | probe |
| --- | --- | --- |
| source commit | `394a8c0245a2d35659711094ca8df9df06494e21` | `25e9265ab79b34d5f84e759cc20bf8f1e16d76cc` |
| source dirty | false | false |
| binary SHA-256 | `84b03fbff9b2f3d6f3a06308512042457759c0facaffda188f800eb9821d77b9` | `eff2cf2de8494be0e15847f405fc451d17dd3656013fb5c4c7f82872acd9f536` |
| report | `control.json.gz` | `probe.json.gz` |
| normal logs | `control.json.q11.block000..004.parod.log.gz` | `probe.json.q11.block000..004.parod.log.gz` |
| diagnostic log | `control.json.q11.diagnostic000.parod.log.gz` | `probe.json.q11.diagnostic000.parod.log.gz` |

Both arms used the same original SF1 Q11 and harness inputs:

- query corpus SHA-256: `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`;
- Q11 SQL SHA-256: `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`;
- source CSV SHA-256: `4c73cc53e1567cad44341276a09589c209919e1002a7a59fbbedbf9d8e7532f3`;
- Paro input snapshot SHA-256: `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`;
- DuckDB database SHA-256: `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`;
- harness SHA-256: `tpcds_compare.py` `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244`,
  `benchmark_evidence.py` `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70`,
  `tpcds_result_contract.py` `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00`,
  and `tpcds_setup.py` `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171`.

The normal cohort used five serial fresh-process blocks, one warmup and one
measurement round per engine, four execution threads, a 2 GiB limit, private Paro
data copies, binary protocol, cache-miss verification, and full typed result/order
validation. Measurement order was seeded-random ABBA within each block. The normal
environment explicitly set `PARO_QUALITY_POLICY_HANDOFF=1` and
`PARO_STATEMENT_CACHE_EVIDENCE=1`; work partition, statement trace, allocation
profile, compile-work evidence, lifecycle trace, and strong-incumbent variables were
unset. `statement_trace=false` and `allocation_profile=false` were verified in the
normal report. The diagnostic cohort had one separate traced block and was excluded
from C1/W.

## Diagnostic counters

The pre-change control binary predates the new goal-scoped counters, so fields added
by this change are absent from its diagnostic log. Existing counters emitted by both
arms are shown for context; the probe-only fields are not presented as a control/probe
delta.

| counter | control | probe |
| --- | ---: | ---: |
| child combination syntheses | 2061 | 2061 |
| child combination recomputes | 149 | 149 |
| published winners | 1285 | 1285 |
| physical subproblem requests | 1203 | 1326 |
| physical subproblem reuse | 1425 | 1413 |
| ReadSet rebuilds (probe instrumentation) | not emitted | 1974 |
| recipe reprocesses (probe instrumentation) | not emitted | 1202 |
| unrelated-goal invalidations | not emitted | 0 |
| related-goal notifications | not emitted | 1725 |
| merged notifications | not emitted | 9974 |
| completion invalidations | not emitted | 47 |

The unchanged synthesis and published-winner counts show that this pilot did not
shrink the candidate frontier to manufacture a speedup. The request/reuse counts are
different and are reported as work-closure observations, not as a claim that every
request is duplicate work. The unit test is the direct evidence for avoiding an
unrelated goal invalidation; the one diagnostic probe does not establish a production
rate for that event.

## Q11 pilot result

All five normal Paro samples per arm verified a cache miss for the target occurrence.
Every measured result passed the 90-row typed schema, value, multiset, and order
contract; Paro and DuckDB result digests matched.

| arm | Paro C1 median / p95 (ms) | DuckDB C1 median / p95 (ms) | C1 ratio (95% CI) | Paro W median / p95 (ms) | DuckDB W median / p95 (ms) | W ratio (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 164.746 / 166.135 | 108.890 / 109.284 | 1.518712 [1.512044, 1.526347] | 70.038 / 91.809 | 105.756 / 130.953 | 0.658766 [0.614326, 0.710592] |
| probe | 170.955 / 172.314 | 108.622 / 109.925 | 1.572850 [1.551992, 1.599144] | 69.967 / 71.671 | 106.369 / 107.143 | 0.659465 [0.654026, 0.665666] |

The probe C1 median is 6.210 ms higher than this five-block control pilot while W is
0.071 ms lower. The pilot is not powered for a causal C1 conclusion, and the exact
goal change did not show a stable end-to-end improvement. It does show no warm
regression in this pilot. The 130.953 ms DuckDB W observation is retained as a valid
slow sample rather than discarded.

Both reports are `EvidenceValid`, but `ModelNotAdmitted` and `MilestoneNotPassed`.
The optimizer state remains `QualityPolicySatisfied + SearchIncomplete`, not
`ProofComplete`. This five-block pilot is not the registered parity or warm
non-inferiority campaign.

## Validation status

- `cargo check -p paro-optimizer`: passed before the isolated commit.
- Exact-goal production-path test and the existing narrowed-child frontier test:
  passed with `--nocapture`.
- Benchmark harness unit tests: 134 passed.
- Full optimizer test compilation on the clean probe worktree is blocked by four
  pre-existing stale fixture calls for the already-dirty resident/staging API
  (`settle_arena_in` identity argument and `StagingInput::Native`/`resident_nodes`);
  these were not changed or blessed. The targeted goal tests pass.
- SQL regress with a server launched under `ulimit -n 65536`: 153 passed, 31 failed,
  0 skipped, 0 new. Failures are existing EXPLAIN/PROFILE shape differences,
  physical-plan/quality fixture differences, and other unmodified contracts. A
  first attempt without inheriting the raised FD limit hit `Too many open files` and
  was rerun with the explicit limit; that first environment failure is not a code
  result.

No default stop policy, search budget, frontier width, cost model, execution path, or
quality gate was changed. Formal M1/M2/parity, compiler <=30 ms, and ProofComplete
remain unachieved.
