# Q11 planning-resident identity v1

Date: 2026-09-15

This archive is the independent control/probe for task 1 of the compiler-cost
round. It measures the query-session resident lowering identity change under the
existing handoff policy. It does not change the optimizer rule set, search
budget, stop policy, cost model, SQL, resource envelope, or execution engine.

## Scope and source identities

| arm | source | binary SHA-256 | role |
| --- | --- | --- | --- |
| control | `d62dfa5ff6133a6a42a6732e5e28e3b4cba28e32` | `072e760ffaba1b175965b2818bedbbaa485601596474f03ec2c4733d499c4e60` | pre-task resident identity |
| probe | `29adcd9f76075feb797979ca5c8e063c0d79b46b` | `37652c0d400d8d7e70294dea4218d9479e23c98b74745e401237026bda985e28` | shared resident lowering identity |

Both arms were built in clean release worktrees. The harness-recorded working-tree
identity was `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
The original SQL is [11.sql](11.sql). Common input identities were:

- data seed `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`;
- DuckDB database `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`;
- query corpus SHA-256 `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`;
- source CSV SHA-256 `4c73cc53e1567cad44341276a09589c209919e1002a7a59fbbedbf9d8e7532f3`;
- harness files were identical: `tpcds_compare.py`
  `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244`,
  `benchmark_evidence.py`
  `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70`,
  `tpcds_result_contract.py`
  `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00`, and
  `tpcds_setup.py`
  `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171`.

The run used `PARO_QUALITY_POLICY_HANDOFF=1`, generator-declared metadata, 4
execution threads, 2 GiB, private per-process data copies, one warmup and three
measurement rounds per process. Normal measurements were fresh-process,
plan-cache-miss and trace-off. Each arm used five serial fresh-process blocks and
one separate diagnostic block. The diagnostic cohort is excluded from C1.

## Normal Q11 results

All 30 measured rows per arm passed typed schema, value, multiset and order
validation. Cache-miss and normal trace-off side channels were verified. Five
blocks are still a pilot and are not the registered 36-block power campaign.

| arm | Paro C1 median / p95 (ms) | DuckDB C1 median / p95 (ms) | C1 ratio (95% CI) | Paro warm median / p95 (ms) | DuckDB warm median / p95 (ms) | warm ratio (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 173.670 / 179.154 | 107.879 / 110.776 | 1.608887 [1.583142, 1.630553] | 68.515 / 70.284 | 105.753 / 106.950 | 0.648166 [0.644042, 0.652360] |
| resident identity | 167.072 / 168.390 | 109.134 / 111.524 | 1.523087 [1.512224, 1.534028] | 69.663 / 79.019 | 106.609 / 109.766 | 0.659748 [0.651700, 0.674622] |

This pilot moves the C1 median by -6.598 ms and the warm median by +1.148 ms
relative to its control; it is not a randomized formal campaign and the DuckDB
warm/C1 values drift between arms. Therefore the C1 movement is directional
evidence, not an isolated causal estimate, and warm non-inferiority is unproven.

## Implementation and attribution

`29adcd9f` removes the production `SettlementCache` private column, binding and
scalar catalogs. Settlement now emits an immutable `ResidentNodeContract` with
operator identity/encoding, scalar roots, output layout and exact input fact
identities. Staging consumes that contract from the same
`PlannerTransformState` identity namespace and validates its layout and columns;
it no longer interns the same operator/scalar identity a second time. Fact
refresh remains separate from structural identity, and savepoint/rollback and
CTE producer/reference mapping remain transactional.

The change is not merely a cache hit: it removes the second identity lowering
and its associated catalog traversal/encoding path. The diagnostic counter
profile also shows a changed work closure: control had 281/430/636 memo
groups/logical/physical entries, 484/437/149 transformation
bindings/apply/inserted, 2500 syntheses and 1436 published candidates; the probe
had 241/365/524, 390/379/124, 2061 syntheses and 1285 published candidates.
That is a plan-search/work-closure change, not an apples-to-apples fixed-work
identity-only proof. The selected plan is semantically valid and all result
checks pass, but the probe's final winner set has five aggregate choices where
the control has four and is not byte-identical. This evidence does not claim
that all C1 reduction came from identity lowering.

The separate diagnostic traces report approximately 74.001 ms optimizer and
74.078 ms compiler-return elapsed for the probe versus 81.777 ms and 81.853 ms
for control. Freeze/handoff were approximately 2.314/1.135 ms for the probe and
1.843/1.195 ms for control. The JSON reports did not enable
`PARO_COMPILE_WORK_EVIDENCE`; their `compile_work` and allocation fields are
therefore empty. No allocation-specific wall-time attribution is claimed.

## Verification

- Settlement focused tests: 19 passed.
- Staging focused tests: 9 passed, including production contract consumption.
- Engine tests: 113 passed for the identity/combination path.
- The clean task2 worktree's full optimizer suite, after adapting only stale test
  fixtures in that temporary validation worktree, had 1322 passed and two known
  undeclared-grant fixture failures; those failures were not blessed.
- Benchmark unit tests: 134 passed under the repository virtual environment.
- SQL regress was run separately on the task2 clean tree with a fresh data dir,
  serially and with `ulimit -n 65536`: 153 passed, 31 failed, 0 skipped. No
  `Too many open files` error occurred in this run. The failures remain
  unblessed expected-plan/output-format and existing grant/identity cases; the
  detailed report is archived with the task2 evidence.

The interactive shell limit was `ulimit -n 256`; no `parod` process remained
after validation. This is reported because FD state can invalidate benchmark
evidence, but it was not changed in the main worktree.

## Completion state

The resident identity contract is implemented and covered by focused tests. The
Q11 pilot preserves complete result correctness and warm performance direction,
but does not establish compiler <=30 ms, formal M1, warm non-inferiority, M2,
parity, or `ProofComplete`. The optimizer status is
`QualityPolicySatisfied + SearchIncomplete`; no budget-limited search is being
relabelled as complete search.

Raw reports and logs are compressed beside this file and verified by
[SHA256SUMS](SHA256SUMS).
