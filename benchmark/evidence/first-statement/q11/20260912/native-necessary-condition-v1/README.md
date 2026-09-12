# Native necessary conditions: experiment registration

2026-09-12. Scope: same-Memo selected-path constant substitution and safe
necessary group-key conditions, with original aggregate residuals and selected
input consumption checks. No budget, cost model, executor or default policy change.

Pilot registration: two fresh blocks for handoff, followed by two fresh blocks
for default control; one warmup and one ABBA measurement round per process.
Original archived Q11; existing immutable SF1 direct seed; 4 threads, 2 GB
(decimal) memory envelope; binary protocol, normal trace-off/cache-miss;
complete typed/order result comparison against DuckDB. Bootstrap 200, seed 1.
One separate trace-on diagnostic block per policy. Preserve all valid samples.
These small sequential cohorts cannot establish a formal parity gate or isolate
machine drift; no formal performance acceptance will be claimed from this pilot.

Target hypothesis: mixed aggregate residuals currently hide a safe input domain.
Producing and actually consuming that domain should improve early candidate
execution quality. Counts and certificates alone do not establish the hypothesis.

Structural EXPLAIN and EXPLAIN ANALYZE sidecars, if collected, are diagnostic
recompilations, not the exact normal SELECT image unless independently matched.

Implementation tests before pilot: native production staging counterexample and
independent SQL 3VL/bag oracle over actual transfer output pass. Transformation
suite: 70 pass, 1 fail (CTE multi-output reservation); investigation pending.
No baseline blessing or user staged changes included.

## Result: execution quality recovered; latency milestone not met

Implementation: `c4c2eea9`. Measured dirty source was based on `33ade4a9` with
this patch plus the pre-existing mixed worktree. The six-file implementation
commit intentionally excludes those pre-existing staged/unstaged dependencies;
it is not an attestation that clean HEAD builds reproduce the experiment.
All six reports attest binary SHA-256
`b085ffee8f5d865388f82649fd41c64163f301a5e96f5319fe51ab9144331c2b`.
Input SQL is the unchanged `../column-transfer-contract-v1/11.sql`, SHA-256
`1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`.

| Pilot | Paro C1 median / p95 ms | DuckDB C1 median / p95 ms | Paired C1 ratio [95% CI] | Paro W median / p95 ms | DuckDB W median ms |
| --- | ---: | ---: | --- | ---: | ---: |
| handoff | 274.872 / 276.988 | 171.046 / 197.108 | 1.625941 [1.383793, 1.910463] | 94.818 / 99.795 | 111.947 |
| default | 1388.393 / 1394.428 | 110.839 / 114.059 | 12.531427 [12.225512, 12.844997] | 112.138 / 129.976 | 106.808 |

Normal C1 samples: handoff Paro `[276.988041, 272.756208]`, DuckDB
`[144.984791, 197.107667]`; default Paro `[1394.428167, 1382.358292]`,
DuckDB `[114.058875, 107.618417]`. No valid slow sample was discarded.
Handoff W ratio `0.857717 [0.837104, 0.881361]`; both policies pass the
90-row typed schema/order/digest checks and post-timer cache-miss checks.
Both reports say `EvidenceValid`, `MilestoneNotPassed`.

The handoff's observed W is close to or better than the same-binary control,
unlike the previous artifact's 179 ms handoff. This is execution-quality
evidence, not a controlled before/after estimate of this patch's speedup.
DuckDB C1 differs markedly between these sequential small cohorts; machine
drift remains a confounder. There is no formal M1, M2 or parity acceptance.

## What changed and what was actually proved

`domain_transfer` now substitutes transparent projection constants using the
existing scalar normalizer. Its AND/OR abstraction derives necessary grouping
conditions without distributing DNF. Unknown atoms mean no restriction, not
an empty set. A mixed predicate can yield both child predicates and its
unchanged output residual. An explicit `unsupported` result cannot certify
completion. Evaluation barriers remain conservative.

Native closure puts the residual back at the aggregate-output boundary, before
returning through a projection. Selected binding follows the derived child
namespace. Quality checks use the exact selected children and actual enforced
Filter predicates, including safe AND coverage; a different UNION branch or a
weaker downstream condition cannot discharge the request. No new cross-Memo
storage, search policy, execution optimization or default-budget change.

Counterfactual unit test: temporarily restoring only the old Constant-projection
rejection made
`production_selected_binding_derives_mixed_aggregate_domain_through_constant_projection`
fail with **0 bindings instead of 1**. Restoring the patch passes and publishes
both the source-key restriction and aggregate-output residual. This reproduces
the source-level production-binding defect; it does not reconstruct the old
benchmark's exact executed DAG (the old archive omitted predicate payloads).

Tests: `domain_` 45 passed; `quality_domain` filter 17 passed; engine tests 102
passed; independent actual-transfer 3VL/bag oracle 4 passed; native-domain
tests including the new production staging counterexample pass. Counts overlap.
Oracle covers constant channels, non-identity renaming, NULL, duplicates,
negative sums, mixed aggregate conditions, foreign/correlated columns and fences.
Full Q11 exercises DECIMAL `SUM(x-y)` through real freeze/execution. We have not
added a complete end-to-end SQL matrix for every requested non-Q11 variation.
`cargo fmt --all -- --check` and `git diff --check` pass.
Benchmark harness unit tests: 126 passed.
Transformation suite remains **70 passed / 1 failed** at
`cte::tests::engine_admits_every_partition_discriminator_from_one_binding`.
It was already recorded in the prior artifact, but that chronology alone does
not prove independence; no baseline was blessed and no all-green claim is made.
Rerunning that test with `PARO_QUALITY_POLICY_HANDOFF` explicitly removed still
fails. Its enabled rule is CTE partitioned materialization, not PredicateTransfer;
the direct necessary-domain/quality route is disabled in that reproducer.
The empty map is successful rule insertions, not the producer's output count.
The fixture already declares output bound two; the assertion's wording is not
a proven one-slot root cause. Which settlement/staging/optional rollback branch
rejects it remains unresolved, including possible shared-path indirect effects.

## Same SELECT diagnostic timeline and remaining mechanism

From `handoff-v1.json.gz`, not a difference between cohort medians:

- Both aggregate-region witnesses first present: 16.063 ms (not input-domain readiness).
- Direct binding first/last: 29.337 / 73.949 ms, 21 dispatches / 204 work units.
- Final selected input joins: logical 246/group 151 at 106.769 ms and
  logical 247/group 152 at 106.923 ms, published by PredicateTransfer.
- Exact root candidate 2420, producer child 2333, consumer child 649;
  root-consumption event 127.457 ms, quality policy 128.433 ms.
- Compiler return 149.804 ms; cumulative search freezing 6.284 ms.
- Lower/admit 0.489 ms; pipeline initialization 0.049 ms;
  first-page duration 138.940 ms; drain duration 139.347 ms;
  SELECT completion elapsed 290.107 ms. Nested times are not added.
- Cost syntheses/new tuples 4,903; recomputes 1,012; rechecks 0;
  fact-value revalidation hits/misses 2/6. These have not decreased against the
  old, lower-quality handoff's 1,221 syntheses. The 21 dispatches are not all
  proved duplicates.

Completion is `QualityPolicySatisfied + SearchIncomplete`; default remains
`BudgetLimited`, neither is `ProofComplete`. The first remaining measured
production gap is the selected restricted partial-aggregate input join variants
still arriving around 107 ms. Consumer-side choices already existed around
17 ms. The experiment therefore does not justify moving to executor tuning or
claiming that the necessary-domain production chain is now early enough.

The bounded lifecycle buffer retained 1,615 records and dropped 19,549. Exact
queue/fact/construction/combination attribution for the late variants is
unresolved; first/last timestamps do not prove continuous waiting. Next single
direction: close this selected restricted-input production/consumption chain
with its residual intact, and establish whether repeated local publication or
ancestor combination is responsible. Do not add global priorities or caches
based solely on these aggregate counts.

## Independent structural/execution sidecars

`handoff-plan-v1.json.gz` shows both date scans with pushed
`d_year = 2001 OR d_year = 2002`, narrow partial aggregates by customer key/year,
dimension joins and final merges; both original mixed year/SUM residuals remain
above the final aggregate, and all four CTE consumers retain their filters.

`handoff-profile-v1.json.gz`: both date scans actually emit 730 rows;
partial aggregate builds consume 1,096,053 / 289,524 rows; final aggregate
builds consume 76,098 / 22,804 rows. These aggregate input counts match the
default profile. Handoff web source emits 719,384 rows versus default 289,524;
handoff store customer source emits 100,000 versus default 61,631. The sidecars
therefore still differ in RF consumption; equality of aggregate counts is not
complete physical-plan equivalence.

Handoff diagnostic admission declares 28,936,512 working-set bytes and four
parallel tasks; this is not measured RSS. Separate profile peak RSS is
448,790,528 bytes (handoff) / 589,037,568 bytes (default), below the watchdog
limit. EXPLAIN ANALYZE times 194.627 / 162.204 ms are diagnostic and include
profiling effects. They do not replace normal C1/W or establish a cold lower bound.
These sidecars are separate compilations; normal SELECT image equality is not
fully attested. The exact diagnostic SELECT child choices remain in the normal
comparison report's independent trace-on cohort.
All 3,210 final-winner choice/child/payload/proof and physical-fingerprint fields
match exactly across handoff's diagnostic SELECT, structural EXPLAIN and profile.
This strengthens their plan association, but still does not attest the trace-off
normal SELECT executable image. Runtime partial aggregates lack direct logical
IDs, so their source association also uses this matching DAG and pipeline topology.

All original JSON and logs are retained, gzip-compressed without content edits.
No expansion of the tracing framework was made. Default production parity and
the 30 ms-quality / 200 ms-C1 milestones remain unfinished.
