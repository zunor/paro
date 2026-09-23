# Demand-driven compile preparation: Q04/Q11/Q74

EvidenceId: `compile-preparation-20260923-v1`. Follow the committed
[registration](../preparation-registration.md). This is a finite pilot, not a
powered performance/parity certificate.

## Outcome

All three architectural changes are implemented. This campaign **does not
demonstrate a Q11 compiler improvement or a 50% reduction**. Q74's aggregate
compiler median is lower; Q04 and Q11 are slightly higher. Retain the negative
result rather than attributing host drift to the implementation.

Normal fresh-process compiler times, milliseconds, six independent blocks per
query/arm (two batches of three):

| Query | Control median | Probe median | Change | Control P90 | Probe P90 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | 19.982 | 21.0185 | +5.19% | 20.811 | 24.241 |
| Q11 | 14.000 | 14.4145 | +2.96% | 15.908 | 15.146 |
| Q74 | 71.4415 | 66.1675 | -7.38% | 79.488 | 66.824 |

P90 uses the maintained collector's `percentile` convention, not a tail
confidence bound. All samples, including Q11 probe's 21.767ms and Q74 control's
87.914ms, remain in the accepted cells. Ordinary timing comes from
`compile.raw.compiler_elapsed_us` of `Executed`, cache-miss receipts; cached
receipts' retained compile work is not counted as another compilation.

| Query | Control C1 | Probe C1 | Control warm | Probe warm | DuckDB C1 during control / probe |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | 270.407 | 291.169 | 174.449 | 176.188 | 134.105 / 146.017 |
| Q11 | 166.006 | 164.283 | 84.994 | 88.179 | 70.423 / 74.865 |
| Q74 | 188.211 | 167.267 | 65.240 | 63.230 | 87.659 / 83.135 |

These are pooled descriptive medians, not ratios of previously published
reports or causal speedup estimates. Each maintained cell also retains its
paired DuckDB samples and block-level uncertainty. Unrelated VM/database
processes were active on the shared host; batch drift is visible in both
engines. No user process was stopped. Warm and Q04/Q74 compiler aggregate
medians did not cross the registered 10% investigation threshold, but that is
**not** a certified non-inferiority pass. No latency target or parity is claimed.

## Implemented contracts

Code: `cf165c2afd71d5cd13b2c582111a038ada1d11c7`.

1. DISTINCT feasibility has an iterative read-only eligibility check owned by
   the same decomposition module as the rewrite. Ineligible queries keep the
   original owned plan; eligible alternatives remain mandatory. The check
   borrows aggregate arguments rather than cloning them.
2. Ordinary/Summary compilation keeps fixed physical-search counters, stop
   evidence and receipts. Only explicit Detail builds the per-goal/frontier
   matrix and walks archived source payloads. Final profiler publication moves
   ownership instead of cloning the finished matrix and rule maps. Absence
   means not collected, not zero.
3. Each immutable `(physical, goal, recipe)` prepares enforcement geometry
   once. Numeric cost, resource feasibility, facts, grants, calibration and
   active-frontier completion still follow their live validation contracts.
   Resumes filter pending recipes before copying handles and fill the reusable
   frontier workspace directly. This is not a new cache of winners or prices.

Both probe Detail captures report the same build/reuse counts:

| Query | Enforcement builds | Reuses | Cost syntheses | Groups / logical / physical |
| --- | ---: | ---: | ---: | --- |
| Q04 | 265 | 68 | 782 | 69 / 77 / 121 |
| Q11 | 215 | 44 | 401 | 53 / 61 / 105 |
| Q74 | 337 | 129 | 1045 | 95 / 136 / 217 |

Synthesis counts, groups, expression counts, selected physical fingerprints,
artifact/structure identities and admitted resource contracts are unchanged
across the arms and blocks. Complete typed results, multiplicities and ordering
pass (6/90/92 rows). Matching identities are association evidence, not an
independent proof of SQL equivalence. Every normal artifact matches that
cell's separate compile capture. COMPILE alone does not execute admission;
its uncovered selected-execution field is not filled from another invocation.

All stops are `QualityPolicySatisfied`, `search_complete=false`: no
`ProofComplete` claim and no search-budget/stop-policy reduction.

## Attribution boundary

The independent Detail cohort retains full physical profiling by design, so
its Finish time cannot measure the ordinary-path matrix removal. Across the
two captures, Q11 Pre is 2.729–3.570ms control and 2.697–3.021ms probe; the
other preparation buckets remain similar. This is not sufficient attribution
of a normal compiler reduction. The exact recipe reuse counters above prove
avoided preparation, not a millisecond saving by themselves.

Q74's second probe capture spends 29.889ms in QualityEvidence, much more than
the four targeted Pre/Recipe/Subproblem/Finish buckets together (8.405ms).
The next investigation should therefore use that existing bounded ledger,
not assume this campaign established a new cache or scheduling bottleneck.
Detailed events are capacity-limited with explicit omissions; top-level rule,
counter and variant omission counts are zero. No complete event-history claim
is made.

## Reproduction and ownership

- Control: `bc5e6d06d96d056be1f2ec8df272b3a5ced27b42`, binary SHA-256
  `b5df2d1e4a9f7ebb1c2bb3385fe5f2be3749704824a05e6e2a57b652dae6fbe4`.
- Probe: `cf165c2afd71d5cd13b2c582111a038ada1d11c7`, binary SHA-256
  `0b4bfbc607e717e23e49be751b6a858fc4d24e104e458527f9d8c8477a305f8f`.
- C/P/C/P batches, Q04/Q11/Q74 in each; one shared Cargo target, clean source
  switching in the existing checkout. No source change, build or task test
  overlapped measurement. Both builds were saved before replacement.
- DuckDB 1.5.5, SF1 immutable seed, generator-declared metadata, binary
  protocol, four threads, 2GB, `quality`, verifier off, default 30-second
  optional deadline. Normal `PARO_COMPILE_WORK_EVIDENCE=1`, statement tracing
  off. No other behavior flags. Resource/observer identities are in inputs.
- `tpcds_compare.py`: three process blocks, one warmup, one ABBA round, one
  independent Detail block, 10,000 bootstrap draws; batch seeds 2026092301
  and 2026092302. Actual CLI, source/build, SQL, harness, data, runtime and
  process identities remain in each unmodified RunOutput.
- Each `<arm>-<batch>-q<query>-run/` owns its campaign/manifest, typed cells
  and single referenced capture. Validators use explicit accepted attempts;
  nothing selects an attempt by newest filename. The twelve RunOutputs total
  about 3.1MiB on disk, below the registered 16MiB limit. No server free-text
  logs or duplicate raw event streams are archived.

## Validation

- Optimizer: 1393 passed. Full workspace: 6932 passed, 0 failed, 85 ignored.
- Workspace/all-targets check and strict Clippy passed.
- Benchmark: 207 passed. Regression harness: 103 passed, 1 skipped.
- All twelve campaign/manifest pairs, normal typed receipts and Detail
  captures passed the maintained schema validators after collection.
- Initial high-FD SQL regress: 183 passed, 2 failed, no SQL result changes.
  Both failures compare allocation-dependent `logical_node_id` literals in
  full-text Rank/CoverDensity and forced-spill TopN profiles. Avoiding an
  unnecessary fork intentionally stops consuming those allocation numbers.
  These fixtures now opt into existing `explain_logical_ids` alpha-renaming,
  which preserves presence and shared-node relationships. The comparator's
  expected-side directives are authoritative, so the same three normalize
  headers are amended in the two `.result` files. All their SQL/plan/row
  payloads, including the historical numeric ids, remain byte-for-byte intact.
  No `.result` is regenerated and no operator, score, spill or result assertion
  is relaxed. Applying only the SQL directives initially left 183/2, also
  preserved as a negative validation result.
  Final fresh-instance full regress: **185 passed, 0 failed, 0 skipped, 0 new**
  in 74.90s, verifier on and FD limit 65536. The saved probe binary's hash was
  unchanged. No server build occurred during the regression run.

Architecture delivered; performance remains **NotCertified**. The matrix does
not justify claiming Q11 <10ms or another 50% improvement.
