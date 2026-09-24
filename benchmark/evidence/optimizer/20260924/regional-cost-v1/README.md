# Shared regional work and predicate activation

## Delivered scope

- Hash work units and calibrated coefficients are shared between join DP and
  physical costing. Expected work ranks both legal build directions; memory
  feasibility, risk and runtime spill contracts remain separate.
- Multi-relation predicates retain their full support, activate only at a
  legal cut, and contribute selectivity once. A small independent exhaustive
  bipartition oracle checks regional DP against its declared cost function.
- The two legal aggregate shapes are each join-optimized before comparison.
  Cost-only selection avoids publishing discarded winner contracts. This is
  **not** full joint `(relation subset, aggregate state)` DP: tree duplication,
  boundary statistics and subtree costing remain. Nor does a shared local
  kernel make the whole-plan RF/phase objective identical to additive DP work.
- Shared integral finite-domain estimation intersects equivalent OR/IN
  restrictions instead of multiplying them. These are estimates, not hard
  bounds or permission to remove predicates.
- Breadth testing found unsupported CROSS_PRODUCT and
  PARTITION_AGGREGATE_WINDOW identity payloads. A subsequent commit adds typed
  encoding and shared nested aggregate encoding, bumps the structure domain
  to v5, and adds real SQL execution coverage. No identity checks were disabled.

Default policy remains `quality`. No Cascades deletion, global optimality,
runtime adaptive build-side switching or default-promotion claim is made.

## Final source matrix C

Clean source `6084d3fcd`; executable SHA256
`5fd10a53ba052a279adbcb3d431133841a678004a5b7f2f94771607cceb52fd5`.
This includes the v5 physical identity coverage fix. The original exploratory
protocol and limitations apply; no thresholds were changed. All six cells
passed complete typed results and the maintained campaign/receipt validators.
These are normal SELECT medians, not diagnostic times.

| Query | Policy | Compiler ms | C1 ms | DuckDB C1 ms | Warm ms | DuckDB warm ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | pipeline | 8.180 | 248.072 | 137.098 | 154.051 | 111.288 |
| Q04 | quality | 19.280 | 231.370 | 154.728 | 132.795 | 113.564 |
| Q11 | pipeline | 4.463 | 131.951 | 70.459 | 80.062 | 56.081 |
| Q11 | quality | 12.399 | 143.666 | 66.606 | 81.104 | 55.269 |
| Q74 | pipeline | 3.860 | 95.491 | 82.786 | 56.829 | 69.974 |
| Q74 | quality | 10.876 | 105.276 | 81.927 | 53.787 | 64.385 |

Compiler samples (microseconds, sorted for presentation; collection order and
every slow sample remain in the cells):

| Query | pipeline | quality |
| --- | --- | --- |
| Q04 | 7885, 8180, 10903 | 17933, 19280, 25060 |
| Q11 | 4420, 4463, 4544 | 12326, 12399, 13273 |
| Q74 | 3782, 3860, 3963 | 10865, 10876, 11103 |

The result is mixed, not completion of the four-task architecture program.
Q04 pipeline warm remains worse than quality and triggers the registered
investigation threshold. Q11 and Q74 compile/C1 favor pipeline in this cohort,
but no query has certified parity. These are policy comparisons within one
binary, not an isolated before/after causal claim for the shared kernel.

After timing ended, separate read-only plan inspection (`selected-plans.txt`,
one bounded four-plan text supplement) confirmed:

- Both Q74 branches again select partial aggregate, dimension join and final
  merge, with date scans estimated at 723 rows. Both policies still carry the
  redundant OR/IN predicate, but no longer multiply its estimated selectivity.
- Q04's consumer now applies a ratio predicate before the last relation is
  joined. Its remaining store/catalog producer joins still build the customer
  input in pipeline, versus the partial aggregate in quality. The store
  partial estimate is 183,392 rows versus 100,000 customer rows. A shared kernel
  alone has not fixed estimation/phase response or chosen the best build side.
- Q04/Q74 CTE/final-aggregate estimates still differ markedly between policies.
  This is direct evidence that a single canonical relation-estimate owner is
  not finished. Do not relabel local work-unit sharing as complete model unity.

This separate inspection suggests next boundaries; it does not causally assign
all warm latency to one operator or substitute for sample-linked diagnostics.

## Final validation

- `RUST_MIN_STACK=33554432 cargo test --workspace --locked -q -- --test-threads=2`:
  passed; optimizer 1,430 passed. Existing ignored tests remain ignored.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- Release server build: passed. Memory-runtime and fallible-vector guards:
  passed. Task-file formatting and `git diff --check`: passed; not a claim
  that all pre-existing repository formatting/header debt is resolved.
- Benchmark tests: 229 passed, one pre-existing pytest return-value warning.
- Real pipeline session tests, including forced-external CTE, cross product,
  partition/global window, NULL, DISTINCT, and shared dimension groups: passed.
- Final-source verifier-on SF1 Q09 and Q12: exact results and receipt/campaign
  validators passed. Their concurrently collected timing is not a latency
  claim. Other failed breadth cases are not silently upgraded to passes.
- Full default SQL regress, FD limit 65,536: 184 passed, one existing settings
  description text failure, zero new cases. The exact expected/actual difference
  is retained in `regress/error.txt`. No expected or `.actual` files updated.

Remaining work is joint aggregate-state DP, one region estimate/physical
response contract (including RF/phase effects), unknown/low-resource policy
coverage, and the broader failed/incomplete corpus gates below. The old
quality search is a bounded comparison reference, not an exhaustive oracle.

## Registered exploratory matrix B

Source `40b912359`, before the subsequent identity-only coverage fix. Each run's
`inputs.json` owns full binary/source/SQL/seed/harness/settings identity. All six
cells use the same binary, DuckDB 1.5.5, four workers and 2GB. Three fresh blocks,
one warmup and two ABBA warm rounds per block; one separate Detail process.
Normal compiler time is the SELECT receipt, never Detail time.

| Query | Policy | Compiler median ms | C1 ms | DuckDB C1 ms | Warm ms | DuckDB warm ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | pipeline | 8.694 | 248.582 | 142.130 | 152.384 | 112.434 |
| Q04 | quality | 18.904 | 247.139 | 133.875 | 142.720 | 111.447 |
| Q11 | pipeline | 4.526 | 141.323 | 72.074 | 81.119 | 55.612 |
| Q11 | quality | 12.398 | 145.357 | 69.937 | 78.844 | 57.557 |
| Q74 | pipeline | 3.909 | 111.822 | 99.532 | 53.949 | 69.383 |
| Q74 | quality | 10.924 | 115.729 | 86.945 | 60.146 | 69.710 |

All 18 cold and 72 warm samples passed complete typed results/bag/ORDER and
receipt checks. Compiler medians favor pipeline, but Q04 execution remains
slower than the same-binary quality reference and the DuckDB baseline. Q11 C1
is still far from DuckDB. Q74's warm result is encouraging, not certification.
Ambient VM/system load, sequential policy cohorts, small process counts and
generator-declared metadata make this **NotCertified**. Do not infer same-plan
causal speedup or formal non-inferiority from these numbers.

Matrix A at `c8501340d` is retained in the unprefixed run directories as a
negative result. Its Q04/Q74 pipeline warm times were about 199/105ms versus
146/71ms for quality. Read-only plan inspection found Q74's duplicated OR/IN
year restriction estimating about seven date rows instead of two years. That
motivated the generic estimator fix and a new cohort, not retrospective sample
exclusion. Different cohorts do not prove the entire warm change was caused
by that fix.

The first in-repository output attempt invalidated its own source identity.
It is retained under `runs/invalid-source-output`, not used as a timing sample.
All subsequent collection wrote outside the checkout before archival.

## Breadth screen: incomplete and not admitted

The verifier-on pipeline-only screen at `40b912359` completed Q01–Q65:
53 exact passes and 12 failed queries. It was an exploratory TPC-DS screen,
not the complete ordered JOB/CEB/TPC-DS/TPC-H/LDBC gate.

- CROSS_PRODUCT identity rejection: Q09/Q23/Q28/Q61.
- PARTITION_AGGREGATE_WINDOW identity rejection: Q12/Q20/Q47/Q53/Q57/Q63.
- Q39: exact row multiset mismatch, missing=42/unexpected=42. The old numeric
  certificate is not automatically valid for this source/plan; no epsilon or
  historical certificate was used to turn it into a pass.
- Q58: ambiguous `item_id` binding. This remains unclassified as to source;
  the screen alone does not establish that this change introduced it.
- Q66: bounded normal cell encoding exceeded its registered 36,096-byte
  capacity. Collection stopped before normal publication. Q67–Q99 were not
  run. The retained campaign is partial, not a completed/accepted campaign.

The screen also exposed poor execution on some supported queries (notably
Q51). These concurrent verifier-on diagnostic timings do not certify latency,
but are sufficient to require exact-plan investigation before promotion.

The identity fix has independent unit/real-entry execution tests; those tests
alone do not recertify the failed SF1 queries. Remaining unsupported resource
envelopes and missing generated JOB/CEB data are explicit coverage gaps.
No expected SQL results, tolerance policies or baselines were updated.

## Evidence ownership

`runs/` contains the maintained RunOutput packages, with normal timings,
sample receipts and a single referenced bounded capture per diagnostic block.
Raw server logs and generated data are not archived. The registration retains
both the original hypothesis and amendment B. A partial campaign must not be
passed through a completion gate merely because its completed cells are valid.
